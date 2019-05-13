#include <nats/nats.h>
#include "options.h"
#include <string>
#include <iostream>
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

static void _publish_ack_cb(const char* guid, const char* error, void* closure) {
    // TODO: delete object from waiting list, so we can check if some object didn't published for a long time
    std::cout << "#Ack#, " << guid << std::endl;
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

int main(int argc, char** argv) {
    opts = parseArgs(argc, argv, usage);
    std::cout << "Sending pipe messages" << std::endl;

    signal(SIGUSR1, sigusr1_handler);

    // Now create STAN Connection Options and set the NATS Options.
    stanConnOptions* connOpts;
    natsStatus s = stanConnOptions_Create(&connOpts);
    if (s == NATS_OK) {
        s = stanConnOptions_SetNATSOptions(connOpts, opts);
    }
    // Create the Connection using the STAN Connection Options
    stanConnection* sc;
    if (s == NATS_OK) {
        s = stanConnection_Connect(&sc, cluster, clientID, connOpts);
    }
    // Once the connection is created, we can destroy the options
    natsOptions_Destroy(opts);
    stanConnOptions_Destroy(connOpts);

    std::string line;
    while (!done && s == NATS_OK) {
        static const auto start = "{\"msg_type\":\"";   // ok, it's ugly. TODO: ?parse json
        static const auto start_len = strlen(start);
        std::getline(std::cin, line);
        if (std::cin.eof()) {
            nats_Sleep(50);
            continue;
        } else {
            if (print) {
                std::cout << line << std::endl;
            }
        }
        if (!line.size()) {
            subj = "bad.empty";
        } else if (0 != line.find(start)) {
            subj = "bad.start";
        } else {
            auto end = line.find('"', start_len);
            subj = std::string::npos == end
                ? "bad.end"
                : line.substr(start_len, end - start_len).c_str();  // TODO: there are forbidden symbols in NATS
        }

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
        s = stanConnection_PublishAsync(sc, subj, line.c_str(), line.size(), _publish_ack_cb, NULL);

        // Note that if this call fails, then we need to free the pubMsg object here since it won't be passed to the ack handler.
        // if (s != NATS_OK)
        //     free(pubMsg);
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
