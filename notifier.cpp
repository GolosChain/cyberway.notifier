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

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

static volatile bool done = false;
const std::string backup_file = "/queue/backup.txt";
const std::string DEFAULT_SOCKET_NAME = "/queue/msg.sock";

boost::asio::io_service io_service;
boost::asio::local::stream_protocol::endpoint ep(DEFAULT_SOCKET_NAME);
boost::asio::local::stream_protocol::socket socket_stream(io_service);

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

    try {
        socket_stream.connect(ep);
        if (socket_stream.native_non_blocking()) {
            socket_stream.native_non_blocking(false);
        }
    } catch (const boost::system::system_error &err) {
        std::cout << DEFAULT_SOCKET_NAME << std::endl;
        std::cerr << "Failed to connect to notifier socket: " << err.what() << std::endl;
        return 1;
    }

    uint64_t msg_index = backup_queue.size();
    boost::asio::streambuf socket_buf;
    boost::system::error_code error;

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

//        try {
//            // json validating
//            std::stringstream local_stream;
//            local_stream << msg.data;
//            boost::property_tree::ptree pt;
//            boost::property_tree::read_json(local_stream, pt);
//        } catch (...) {
//            std::cerr << "Data error: " << msg.data << std::endl;
//            throw;
//        }

        if (print) {
            std::cout << msg.data << std::endl;
        }

        msg.index   = msg_index++;
        msg.subject = get_subject(msg.data);
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
    }

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
