/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include <random>
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define GOSSIP_PAYLOAD_SIZE 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
	PINGREQ,
	PINGREP,
    DUMMYLASTMSGTYPE
};

template<typename T>
T random(T range_from, T range_to) {
    std::random_device                  rand_dev;
    std::mt19937                        generator(rand_dev());
    std::uniform_int_distribution<T>    distr(range_from, range_to);
    return distr(generator);
}

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

typedef struct GossipMembershipEntry {
	int id;
	short port;
	long heartbeat;
}GossipMembershipEntry;

typedef struct GossipMessage {
	MessageHdr header;
	Address sender;
	short number_of_entries;
	GossipMembershipEntry entries[GOSSIP_PAYLOAD_SIZE];
}GossipMessage;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
	void sendMessage (MsgTypes msgtype, int id, short port);
	void sendMessage (MsgTypes msgtype, Address *destination);
	void processGossipMessage (GossipMessage *msg);
	short loadGossipEntries(GossipMembershipEntry entries[]);
	void updateMemberList (int id, short port,	long heartbeat);
	void sendPing();

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	int getIdFromAddress(Address *addr);
	short getPortFromAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
