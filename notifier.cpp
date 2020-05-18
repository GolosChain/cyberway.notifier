#include <nats/nats.h>
#include "options.h"
#include <string>
#include <iostream>
#include <memory>
#include <signal.h>
#include <fstream>

#include <map>
#include <tuple>
#include <vector>
#include <mutex>
#include <functional>

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

static volatile bool done = false;
const std::string backup_file = "/queue/backup.txt";
const std::string DEFAULT_SOCKET_NAME = "/queue/msg.sock";

boost::asio::io_service io_service;
boost::asio::local::stream_protocol::endpoint ep(DEFAULT_SOCKET_NAME);
boost::asio::local::stream_protocol::socket socket_stream(io_service);

static volatile uint64_t attempts = 0;
static volatile bool startup = true, working;

struct message final {
    uint64_t    index;
    std::string subject;
    std::string data;
}; // struct message

using message_map = std::map<uint64_t, message>;

message_map msgs_queue;
std::mutex  msgs_mutex; // Protect queue from different threads

std::string get_subject(const std::string& data) {
    std::string subject;
    static const auto start = "{\"msg_channel\":\"";   // ok, it's ugly. TODO: ?parse json
    static const auto start_len = strlen(start);
    if (!data.size()) {
        subject = "bad.empty";
    } else if (0 != data.find(start)) {
        subject = "bad.start";
    } else {
        auto end = data.find('"', start_len);
        subject = std::string::npos == end
            ? "bad.end"
            : data.substr(start_len, end - start_len).c_str();  // TODO: there are forbidden symbols in NATS
    }
    return subject;
}

message_map fill_backup_msgs() {
    message_map queue;

    std::ifstream backup(backup_file);
    if (!backup.is_open()) {
        return queue;
    }

    if (backup.tellg() == std::ifstream::traits_type::eof()) {
        return queue;
    }

    for (uint64_t i = 0; ; ++i) {
        message msg;
        if (!std::getline(backup, msg.subject) || !std::getline(backup, msg.data)) {
            break;
        }
        queue.emplace(i, std::move(msg));
    }

    backup.close();
    return queue;
}

static void sigusr1_handler(int signum) {
    print = !print;
}

void create_backup_file() {
    std::fstream backup(backup_file, std::ios::out | std::ios::app);
    if (backup.is_open()) {
        for (auto& obj : msgs_queue) {
            backup << obj.second.subject << std::endl;
            backup << obj.second.data << std::endl;
        }
    }
    backup.close();
}

static void sig_int_term_handler(int signum) {
    if (signum == SIGINT) {
        std::cerr << "Interrupt signal (" << signum << ") received." << std::endl;
    } else if (signum == SIGTERM) {
        std::cerr << "Termination signal (" << signum << ") received." << std::endl;
    }
    done = true;
}

static void _nats_publish_ack_cb(const char*, const char* error, void* closure) {
    std::lock_guard<std::mutex> guard(msgs_mutex);

    auto index = (uint64_t)closure;
    if (error != NULL) {
        std::cerr << "Nats send error: " << error << std::endl;
        done = true;
    } else {
        msgs_queue.erase(index);
    }
}

static void _nats_connection_lost_cb(stanConnection*, const char* errTxt, void*) {
    std::cerr << "Connection lost: " << errTxt << std::endl;
    if (!startup) {
        if (attempts < maxAttempts || !maxAttempts) {
            attempts ++;
            if (!maxAttempts)
                std::cerr << "Trying to reconnect... Attempt " << attempts << "." << std::endl;
            else
                std::cerr << "Trying to reconnect... Attempt " << attempts << " of " << maxAttempts << "." << std::endl;
            sleep(attempts);
            working = true;
        }
        else {
            std::cerr << "Out of attempts." << std::endl;
            working = false;
        }
    }
}

static natsStatus send_nats_message(stanConnection* sc, stanConnOptions* connOpts, const message& msg) {
    natsStatus s = NATS_OK;

    for (int i = 0; i < 24 * 1000; ++i) {
        if (async) {
            s = stanConnection_PublishAsync(sc, msg.subject.c_str(), msg.data.c_str(), msg.data.size(), _nats_publish_ack_cb, (void*)msg.index);
        } else {
            s = stanConnection_Publish(sc, msg.subject.c_str(), msg.data.c_str(), msg.data.size());
        }

        if (s == NATS_CONNECTION_CLOSED) {
            s = stanConnection_Connect(&sc, cluster, clientID, connOpts);
        }

        if (s == NATS_TIMEOUT) {
            nats_Sleep(50);
            continue;
        }
        break;
    }

    return s;
}


typedef std::function<bool(stanConnection*, stanSubscription*, const char*, stanMsg*)> onMsgFunction;

const uint64_t SEQ_LAST = ~(uint64_t)0;
const uint64_t SEQ_ALL = (uint64_t)0;

natsStatus getMessagesFromNats(stanConnection *sc, const char *channel, uint64_t deliverSeq, onMsgFunction onMsg, std::optional<int64_t> timeout = {}) {
    stanSubOptions *subOpts = NULL;
    stanSubscription *sub = NULL;
    natsStatus s;
    struct Status {
        onMsgFunction onMsg;
        bool cont;
    } status{onMsg, true};

    s = stanSubOptions_Create(&subOpts);

    if (s == NATS_OK) {
        if (deliverSeq == SEQ_LAST) {
            s = stanSubOptions_StartWithLastReceived(subOpts);
        } else if (deliverSeq == SEQ_ALL) {
            s = stanSubOptions_DeliverAllAvailable(subOpts);
        } else {
            s = stanSubOptions_StartAtSequence(subOpts, deliverSeq);
        }
    }

    if (s == NATS_OK)
        s = stanSubOptions_SetMaxInflight(subOpts, 4*1024);

    if (s == NATS_OK)
        s = stanConnection_Subscribe(&sub, sc, channel, [](stanConnection *sc, stanSubscription *sub, const char *channel, stanMsg *msg, void *closure) {
                Status *st = static_cast<Status*>(closure);
                if (st->onMsg(sc, sub, channel, msg) == false) {
                    st->cont = false;
                }
                stanMsg_Destroy(msg);
            }, &status, subOpts);

    stanSubOptions_Destroy(subOpts);

    int64_t start = nats_Now();

    if (s == NATS_OK) {
        while (!done && status.cont) {
            if (timeout.has_value()) {
                elapsed = nats_Now() - start;
                if (elapsed > timeout) break;
            }
            nats_Sleep(15);
        }
    }

    if (sub != NULL) 
        s = stanSubscription_Close(sub);

    return s;
}

struct BlockId {
    unsigned number = 0;
    std::string id = {};
};

natsStatus getLastCommitBlock(stanConnection *sc, BlockId &commitBlockId, BlockId &lastBlockId) {
    uint64_t lastSequence = 0;
    natsStatus s;

    s = getMessagesFromNats(sc, "Blocks", SEQ_LAST, [&](stanConnection *sc, stanSubscription *sub, const char *channel, stanMsg *msg)->bool {
            lastSequence = stanMsg_GetSequence(msg);
            return false;
        }, 1000);
    if (s != NATS_OK) return s;

    while(commitBlockId.number == 0 && lastSequence > 0) {
        uint64_t startSequence = (lastSequence < 100) ? 0 : lastSequence-100;
        s = getMessagesFromNats(sc, "Blocks", startSequence, [&](stanConnection *sc, stanSubscription *sub, const char *channel, stanMsg *msg)->bool {
                namespace pt = boost::property_tree;
                uint64_t sequence = stanMsg_GetSequence(msg);
                std::string data(stanMsg_GetData(msg), stanMsg_GetDataLength(msg));
                std::stringstream ss(data);
                pt::ptree root;

                try {
                    pt::read_json(ss, root);
                    std::string msg_type = root.get<std::string>("msg_type");
                    unsigned block_num = (msg_type == "AcceptTrx") ? 0 : root.get<unsigned>("block_num");
                    std::string id = root.get<std::string>("id");
                    if ((msg_type == "CommitBlock" || msg_type == "AcceptBlock") && (lastBlockId.number < block_num)) lastBlockId = {block_num, id};
                    if ((msg_type == "CommitBlock") && (commitBlockId.number < block_num)) commitBlockId = {block_num, id};
                } catch (const pt::ptree_error& err) {}

                return sequence < lastSequence;
            }, 10*1000);
        if (s != NATS_OK) return s;

        lastSequence = startSequence;
    }

    return NATS_OK;
}

void processing(boost::asio::streambuf &socket_buf, boost::system::error_code &error, uint64_t &msg_index, BlockId &lastBlockId, BlockId &commitBlockId, natsStatus &s, stanConnection* &sc, stanConnOptions* &connOpts) {
    for (;;) {
        boost::asio::read_until(socket_stream, socket_buf, "\n", error);
        if (error) {
            // std::cerr << "Receive failed: " << error.message() << std::endl;
            // nodeos shutdowns
            break;
        }

        message msg;
        std::istream data_stream(&socket_buf);
        std::getline(data_stream, msg.data);

        if (print) {
            std::cout << msg.data << std::endl;
        }

        msg.index   = msg_index++;
        msg.subject = get_subject(msg.data);

        if (lastBlockId.number || commitBlockId.number) {
            if (msg.subject != "Blocks") {
                std::cerr << "Invalid message subject '" << msg.subject << "' for synchronize mode" << std::endl;
                break;
            }

            namespace pt = boost::property_tree;
            try {
                std::stringstream ss(msg.data);
                pt::ptree root;
                pt::read_json(ss, root);
                std::string msg_type = root.get<std::string>("msg_type");
                std::string id = root.get<std::string>("id");
                unsigned block_num = (msg_type == "AcceptTrx") ? commitBlockId.number : root.get<unsigned>("block_num");

                if (block_num < commitBlockId.number) continue;

                if (commitBlockId.number && msg_type == "CommitBlock") {
                    if (block_num == commitBlockId.number) {
                        if (id != commitBlockId.id) {
                            std::cerr << "Invalid last commit block in synchronize mode. Expect: " << commitBlockId.id << ", get: " << id << std::endl;
                            break;
                        } else {
                            std::cerr << "Found last commit block in synchronize mode: " << commitBlockId.id << std::endl;
                            commitBlockId = BlockId();    // Disable check for commit block
                            continue;
                        }
                    } else if (block_num == commitBlockId.number + 1) {
                        std::cerr << "Continue with next commit block in synchronize mode: " << id << std::endl;
                        commitBlockId = BlockId();    // Disable check for commit block
                    } else {
                        std::cerr << "Invalid commit block_num in synchronize mode. Expect: " << commitBlockId.number << ", get: " << block_num << std::endl;
                        break;
                    }
                }

                if (lastBlockId.number) {
                    if (block_num > lastBlockId.number + 1) {
                        std::cerr << "Too large block_num in message for synchronize mode. Last: " << lastBlockId.number << ", current: " << block_num << std::endl;
                        break;
                    } else lastBlockId = BlockId();
                }

            } catch (const pt::ptree_error &err) {
                std::cerr << "Invalid json in synchronize mode: " << err.what() << std::endl;
                std::cerr << msg.data << std::endl;
                break;
            }
        }

        std::pair<message_map::iterator, bool> result;
        {
            std::lock_guard<std::mutex> guard(msgs_mutex);
            result = msgs_queue.emplace(msg.index, std::move(msg));
        }

        if (s == NATS_OK) {
            s = send_nats_message(sc, connOpts, result.first->second);
            if (s != NATS_OK || done) {
                std::cerr << "Shutdown" << std::endl;
                socket_stream.shutdown(socket_stream.shutdown_both, error);
            } else if (!async) {
                msgs_queue.erase(result.first->first);
            }
        }

        startup = false;
        attempts = 0;
    }
}


int main(int argc, char** argv) {
    opts = parseArgs(argc, argv, "");
    
    std::cerr << "Sending socket messages" << std::endl;

    signal(SIGUSR1, sigusr1_handler);
    signal(SIGINT,  sig_int_term_handler);
    signal(SIGTERM, sig_int_term_handler);

    // Now create STAN Connection Options and set the NATS Options.
    stanConnOptions* connOpts = nullptr;
    natsStatus s = stanConnOptions_Create(&connOpts);
    if (s == NATS_OK) {
        s = stanConnOptions_SetNATSOptions(connOpts, opts);
    }
    if (s == NATS_OK) {
        s = stanConnOptions_SetPings(connOpts, 5 /* seconds */, 24 /* maximum missed pings */);
    }
    if (s == NATS_OK) {
        s = stanConnOptions_SetPubAckWait(connOpts, 120 * 1000 /* ms */);
    }
    if (s == NATS_OK) {
        s = stanConnOptions_SetConnectionLostHandler(connOpts, _nats_connection_lost_cb, nullptr);
    }
    stanConnection* sc = nullptr;
    if (s == NATS_OK) {
        s = stanConnection_Connect(&sc, cluster, clientID, connOpts);
    }
    natsOptions_Destroy(opts);

    auto backup_queue = fill_backup_msgs();
    for (const auto& msg: backup_queue) {
        s = send_nats_message(sc, connOpts, msg.second);
        if (s != NATS_OK) {
            return 2;
        }
    }
    std::remove(backup_file.c_str());

    BlockId commitBlockId, lastBlockId;
    {
        natsStatus s2;
        std::cerr << "Receve last & commit block from nats" << std::endl;
        s2 = getLastCommitBlock(sc, commitBlockId, lastBlockId);
        if (s2 != NATS_OK) {
            return 2;
        }
        std::cerr << "CommitBlock: " << commitBlockId.number << ", " << commitBlockId.id << std::endl;
        std::cerr << "  LastBlock: " <<   lastBlockId.number << ", " <<   lastBlockId.id << std::endl;
    }

    try {
        socket_stream.connect(ep);
        if (socket_stream.native_non_blocking()) {
            socket_stream.native_non_blocking(false);
        }
    } catch (const boost::system::system_error &err) {
        std::cerr << "Failed to connect to notifier socket '" << ep.path() << "': " << err.what() << std::endl;
        return 1;
    }

    uint64_t msg_index = backup_queue.size();
    boost::asio::streambuf socket_buf;
    boost::system::error_code error;

    do {
        processing(socket_buf, error, msg_index, lastBlockId, commitBlockId, s, sc, connOpts);
    }
    while(working);

    if (s != NATS_OK) {
        std::cerr << "Nats error: " << s << " - " << natsStatus_GetText(s) << std::endl;
        nats_PrintLastErrorStack(stderr);
    }

    stanConnOptions_Destroy(connOpts);
    stanConnection_Close(sc);
    stanConnection_Destroy(sc);
    nats_Sleep(50);    // To silence reports of memory still in-use with valgrind.
    nats_Close();

    if (s != NATS_OK) {
        create_backup_file();
    }
    return 0;
}
