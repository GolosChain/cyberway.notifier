#include <nats/nats.h>
#include "options.h"
#include <string>
#include <iostream>
#include <memory>
#include <signal.h>

static const char* usage =
    "-txt           text to send (default is 'hello')\n";

struct myPubMsgInfo {
    const char* payload;
    int         size;
    char        ID[30];
};

static volatile bool done = false;
// natsOptions* opts   = NULL;
// const char* cluster    = "cyberway";
// const char* clientID   = "notifier";
// const char* subj   = "foo";
// const char* txt    = "hello";


struct message final {
    std::string subject;
    std::string data;
}; // struct message

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

static void _publish_ack_cb(const char* guid, const char* error, void* closure) {
    std::unique_ptr<message> msg(static_cast<message*>(closure));
    // TODO: delete object from waiting list, so we can check if some object didn't published for a long time
    //std::cout << "#Ack#, " << guid << std::endl;
    // myPubMsgInfo* pubMsg = (myPubMsgInfo*)closure;
    // printf("Ack handler for message ID=%s Data=%.*s GUID=%s - ", pubMsg->ID, pubMsg->size, pubMsg->payload, guid);
    if (error != NULL) {
        std::cout << "Error: " << error << std::endl;
        done = true;    // TODO: locking
    }
    // free(pubMsg);    // This is a good place to free the pubMsg info since we no longer need it
    // Notify the main thread that we are done. This is not the proper way and you should use some locking.
    // done = true;
}

static void sigusr1_handler(int signum) {
    print = !print;
}

static void sig_int_term_handler(int signum) {
    if (signum == SIGINT)
        std::cout << "Interrupt signal (" << signum << ") received." << std::endl;
    else if (signum == SIGTERM)
        std::cout << "Termination signal (" << signum << ") received." << std::endl;
    done = true;
}

int main(int argc, char** argv) {
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
    // Create the Connection using the STAN Connection Options
    stanConnection* sc;
    if (s == NATS_OK) {
        s = stanConnection_Connect(&sc, cluster, clientID, connOpts);
    }
    // Once the connection is created, we can destroy the options
    natsOptions_Destroy(opts);
    stanConnOptions_Destroy(connOpts);

    while (!done && s == NATS_OK) {
        auto msg = std::make_unique<message>();
        std::getline(std::cin, msg->data);
        if (std::cin.eof()) {
            nats_Sleep(50);
            continue;
        } else {
            if (print) {
                std::cout << msg->data << std::endl;
            }
        }
        msg->subject = get_subject(msg->data);

        // TODO: create object to check in ack
        // TODO: cpp
        // myPubMsgInfo* pubMsg = (myPubMsgInfo*)calloc(1, sizeof(myPubMsgInfo));
        // if (pubMsg == NULL)
        //     s = NATS_NO_MEMORY;
        // if (s == NATS_OK) {
        //     pubMsg->payload = txt;
        //     pubMsg->size = (int)strlen(txt);
        //     snprintf(pubMsg->ID, sizeof(pubMsg->ID), "%s:%d", "xyz", 234);
        // }
        // if (s == NATS_OK) {
        // s = stanConnection_PublishAsync(sc, subj, pubMsg->payload, pubMsg->size, _pubAckHandler, (void*)pubMsg);
        for (int i = 0; i < 24 * 1000; ++i) {
            if (async) {
                s = stanConnection_PublishAsync(sc, msg->subject.c_str(), msg->data.c_str(), msg->data.size(), _publish_ack_cb, msg.get());
            } else {
                s = stanConnection_Publish(sc, msg->subject.c_str(), msg->data.c_str(), msg->data.size());
            }
            if (s == NATS_TIMEOUT) {
                nats_Sleep(50);
                continue;
            }
            break;
        }

        // Note that if this call fails, then we need to free the pubMsg object here since it won't be passed to the ack handler.
        if (s == NATS_OK && async) {
            msg.release();
        }

    }

    if (s != NATS_OK) {
        std::cout << "Error: " << s << " - " << natsStatus_GetText(s) << std::endl;
        nats_PrintLastErrorStack(stderr);
    }

    stanConnection_Destroy(sc);
    // nats_Sleep(50);    // To silence reports of memory still in-use with valgrind.
    nats_Close();

    return 0;
}
