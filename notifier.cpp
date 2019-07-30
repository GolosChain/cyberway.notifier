#include <nats/nats.h>
#include "options.h"
#include <string>
#include <iostream>
#include <memory>
#include <signal.h>
#include <fstream>
#include <map>
#include <tuple>

static const char* usage =
    "-txt           text to send (default is 'hello')\n";

struct myPubMsgInfo {
    const char* payload;
    int         size;
    char        ID[30];
};

static volatile bool done = false;
const std::string backup_file = "/queue/backup.txt";
// natsOptions* opts   = NULL;
// const char* cluster    = "cyberway";
// const char* clientID   = "notifier";
// const char* subj   = "foo";
// const char* txt    = "hello";


struct message final {
    std::string subject;
    std::string data;
}; // struct message

std::map<uint64_t, message> msgs_queue;

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

bool is_file_empty(std::ifstream& file) {
    return file.tellg() == std::ifstream::traits_type::eof();
}

void fill_backup_msgs() {
    std::ifstream backup(backup_file);
    if (backup.is_open()) {
        if (is_file_empty(backup))
            return;

        for (auto [i, line_data, line_subject] = std::tuple<uint64_t, std::string, std::string>{0, "", ""};
             std::getline(backup, line_subject), std::getline(backup, line_data); i++) {
            msgs_queue.insert({ i, {line_subject, line_data} });
        }
        backup.close();
        std::remove(backup_file.c_str());
    }
}

static void _publish_ack_cb(const char* guid, const char* error, void* closure) {
    // TODO: delete object from waiting list, so we can check if some object didn't published for a long time
    //std::cout << "#Ack#, " << guid << std::endl;
    // myPubMsgInfo* pubMsg = (myPubMsgInfo*)closure;
    // printf("Ack handler for message ID=%s Data=%.*s GUID=%s - ", pubMsg->ID, pubMsg->size, pubMsg->payload, guid);

    if (error != NULL) {
        std::cout << "Error: " << error << std::endl;
        done = true;    // TODO: locking
    } else {
        auto index = *(static_cast<uint64_t*>(closure));
        msgs_queue.erase(index);
    }
    // free(pubMsg);    // This is a good place to free the pubMsg info since we no longer need it
    // Notify the main thread that we are done. This is not the proper way and you should use some locking.
    // done = true;
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
    if (signum == SIGINT)
        std::cout << "Interrupt signal (" << signum << ") received." << std::endl;
    else if (signum == SIGTERM)
        std::cout << "Termination signal (" << signum << ") received." << std::endl;
    done = true;
}

static void
connectionLostCB(stanConnection *sc, const char *errTxt, void *closure)
{
    bool *connLost = (bool*) closure;

    printf("Connection lost: %s\n", errTxt);
    *connLost = true;
}

int main(int argc, char** argv) {
    bool connLost = false;
    opts = parseArgs(argc, argv, usage);
    std::cout << "Sending pipe messages" << std::endl;

    signal(SIGUSR1, sigusr1_handler);
    signal(SIGINT, sig_int_term_handler);
    signal(SIGTERM, sig_int_term_handler);

    // Now create STAN Connection Options and set the NATS Options.
    stanConnOptions* connOpts;
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
        s = stanConnOptions_SetConnectionLostHandler(connOpts, connectionLostCB, (void*)&connLost);
    }

    // Create the Connection using the STAN Connection Options
    stanConnection* sc = nullptr;
    if (s == NATS_OK) {
        s = stanConnection_Connect(&sc, cluster, clientID, connOpts);
    }

    // Once the connection is created, we can destroy the options
    natsOptions_Destroy(opts);
    stanConnOptions_Destroy(connOpts);

    auto lambda_send_message = [&](const uint64_t* index, const message& msg) {
        for (int i = 0; i < 24 * 1000; ++i) {
            if (async) {
                s = stanConnection_PublishAsync(sc, msg.subject.c_str(), msg.data.c_str(), msg.data.size(), _publish_ack_cb, const_cast<uint64_t*>(index));
            } else {
                s = stanConnection_Publish(sc, msg.subject.c_str(), msg.data.c_str(), msg.data.size());
            }

            if (s == NATS_TIMEOUT) {
                nats_Sleep(50);
                continue;
            }
            break;
        }
    };

    fill_backup_msgs();
    for (const auto& item : msgs_queue) {
        if (s != NATS_OK)
            break;

        lambda_send_message(&item.first, item.second);
    }

    bool warn = false;

    while (s == NATS_OK) {
        std::string data;
        std::getline(std::cin, data);

        if (done) {
            if (data.size()) {
                if (!warn) {
                    std::cerr << "WARNING! Pipe hasn't empty." << std::endl;
                    warn = true;
                }
            } else {
                std::cout << "break" << std::endl;
                break;
            }
        }

        if (std::cin.eof() && !msgs_queue.size()) {
            nats_Sleep(50);
            continue;
        } else if (print)
            std::cout << data << std::endl;

        uint64_t index = msgs_queue.size();
        auto [it, status] = msgs_queue.insert({ index, {get_subject(data), data} });
        const auto& msg = it->second;

        lambda_send_message(&it->first, msg);

        // Note that if this call fails, then we need to free the pubMsg object here since it won't be passed to the ack handler.
        if (s == NATS_OK && !async) {
            msgs_queue.erase(index);
        }
    }

    if (!connLost && (sc != NULL)) {
        natsStatus closeSts = stanConnection_Close(sc);
        if ((s == NATS_OK) && (closeSts != NATS_OK))
            s = closeSts;
    }

    if (s != NATS_OK) {
        std::cout << "Error: " << s << " - " << natsStatus_GetText(s) << std::endl;
        nats_PrintLastErrorStack(stderr);
        create_backup_file();
    }

    stanConnection_Destroy(sc);
    nats_Sleep(50);    // To silence reports of memory still in-use with valgrind.
    nats_Close();

    return 0;
}
