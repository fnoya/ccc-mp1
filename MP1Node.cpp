/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	GossipMessage *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg = (GossipMessage *) malloc(sizeof(GossipMessage) + 1);
        msg->header.msgType = JOINREQ;
        msg->sender = memberNode->addr;
        msg->number_of_entries = 1;
        msg->entries[0].id = getIdFromAddress(&memberNode->addr);
        msg->entries[0].port = getPortFromAddress(&memberNode->addr);
        msg->entries[0].heartbeat = memberNode->heartbeat;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(GossipMessage));

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

int MP1Node::getMostRecentMember() {
    int pos = 1;
    long ts = 0;
    for (int i=1; i<memberNode->memberList.size(); i++) {
        if (memberNode->memberList.at(i).gettimestamp() > ts){
            ts = memberNode->memberList.at(i).getheartbeat();
            pos = i;
        }
    }
    return pos;
}

int MP1Node::getOldestMember() {
    int pos = 1;
    long ts = memberNode->heartbeat;
    for (int i=1; i<memberNode->memberList.size(); i++) {
        if (memberNode->memberList.at(i).gettimestamp() < ts){
            ts = memberNode->memberList.at(i).getheartbeat();
            pos = i;
        }
    }
    return pos;
}

void MP1Node::updateMemberList (int id, short port,	long heartbeat) {

    if (id == getIdFromAddress(&memberNode->addr))  // It's me, just return my entry
        return;

    MemberListEntry *found = NULL;
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); found==NULL && it != memberNode->memberList.end(); ++it) {
        if (id == it->getid() && port == it->getport())
            found = &(*it);
    }
    if (found == NULL) {
        // Member not found in List, add
        Address addedadr = createAddressFromIdPort(id, port);
        log->logNodeAdd(&memberNode->addr, &addedadr);
        if (GOSSIP_PAYLOAD_SIZE > memberNode->memberList.size()) {
            MemberListEntry *newmember = (MemberListEntry *) malloc(sizeof(MemberListEntry) + 1);
            newmember->setid(id);
            newmember->setport(port);
            newmember->setheartbeat(heartbeat);
            newmember->settimestamp(memberNode->heartbeat);
            memberNode->memberList.push_back(*newmember); 
            free(newmember);
        } else {
            // List full, replace one
            //long pos = random(1, GOSSIP_PAYLOAD_SIZE-1);  // Random
            long pos = getOldestMember();
            Address addrtoberemoved = createAddressFromIdPort(memberNode->memberList.at(pos).getid(), memberNode->memberList.at(pos).getport());
            log->logNodeRemove(&memberNode->addr, &addrtoberemoved);
            memberNode->memberList.at(pos).setid(id);
            memberNode->memberList.at(pos).setport(port);
            memberNode->memberList.at(pos).setheartbeat(heartbeat);
            memberNode->memberList.at(pos).settimestamp(memberNode->heartbeat);
        }
    } else {
        if (found->getheartbeat() < heartbeat) {
            found->setheartbeat(heartbeat);
            found->settimestamp(memberNode->heartbeat);
        }
    }
    return;
}

short MP1Node::loadGossipEntries (GossipMembershipEntry entries[]) {
    int pos = 0;

    memset(entries, 0, sizeof(GossipMessage::entries));
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        if (it->gettimestamp() >= memberNode->heartbeat - TFAIL) {  // do not propagate failed nodes
            entries[pos].id = it->getid();
            entries[pos].port = it->getport();
            entries[pos++].heartbeat = it->getheartbeat();
        }
    }
    return (pos);
}

void MP1Node::processGossipMessage (GossipMessage *msg) {
    for (int i=msg->number_of_entries-1; i >= 0; i--) {  //Reverse order so sender always is kept or added to the list
        GossipMembershipEntry *entry = &msg->entries[i];
        updateMemberList(entry->id, entry->port, entry->heartbeat);
    }
}

Address MP1Node::createAddressFromIdPort(int id, short port) {
    static char s[8];
    sprintf(s, "%d:%d", id, port);
    Address *addr = new Address(s);
    return *addr;
}

void MP1Node::sendMessage (MsgTypes msgtype, int id, short port) {
    Address destinationaddr = createAddressFromIdPort(id, port);
    sendMessage(msgtype, &destinationaddr);
}

void MP1Node::sendMessage (MsgTypes msgtype, Address *destination) { 
    GossipMessage* response;
    response = (GossipMessage *) malloc(sizeof(GossipMessage) + 1);
    MessageHdr* hdr = (MessageHdr *) &response->header;
    hdr->msgType = msgtype;
    vector<MemberListEntry> memberlist = memberNode->memberList;

    response->number_of_entries = loadGossipEntries(response->entries);
    response->sender = memberNode->addr;
    

    //size_t msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long) + 1;
    //msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    
    // msg->msgType = JOINREP;
    // memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(Address));
    // memcpy((char *)(msg+1) + 1 + sizeof(Address), &memberNode->heartbeat, sizeof(long));
    emulNet->ENsend(&memberNode->addr, destination, (char *)response, sizeof(GossipMessage));
    free(response);
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
#ifdef DEBUGLOG
    static char s[1024];
#endif

        GossipMessage* msg;
        msg = (GossipMessage *) data;
        MessageHdr* hdr = (MessageHdr *) &msg->header;
        switch (hdr->msgType) {
            case JOINREQ: {
                printf("JOINREQ\n");
                sendMessage (JOINREP, &msg->sender); 
                processGossipMessage(msg); 
                break;
            }
            case JOINREP: {
                printf("JOINREP\n");
                processGossipMessage(msg);
                memberNode->inGroup = true;
                break;
            }
            case PINGREQ: {
                printf("PINGREQ\n");
                sendMessage(PINGREP, &msg->sender);
                processGossipMessage(msg);
                break;
            }
            case PINGREP: {
                printf("PINGREP\n");
                processGossipMessage(msg);
                break;
            }
            case DUMMYLASTMSGTYPE: {
                break;
            }
        }
#ifdef DEBUGLOG
        sprintf(s, "Received message...");
        log->LOG(&memberNode->addr, s);
        Address* sender = &msg->sender;
        printAddress(sender);
        printf("--> ");
        printAddress(&memberNode->addr);
        printf("\n");
        int i; for (i = 0; i < size; i++) { if (i > 0) printf(":"); printf("%02X", data[i]); } printf("\n");
        printf("------FIN recvCallBack-----\n");
#endif

    return(true);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    memberNode->heartbeat++;
    memberNode->memberList.at(0).setheartbeat(memberNode->heartbeat);  // Update my heartbeat in member list
    memberNode->memberList.at(0).settimestamp(memberNode->heartbeat);  // Update my timestamp also
    printNodes();
    cleanFailedNodes();
    if (memberNode->memberList.size() > 1) 
        sendPing();
    else {
        #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "NODE WITH NO MEMBERS IN MEMBER LIST!");
        #endif
        Address joinaddress = getJoinAddress();
        introduceSelfToGroup(&joinaddress);
        return;
    }

}

void MP1Node::cleanFailedNodes() {
    Address addrtoberemoved;
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    while (it != memberNode->memberList.end()) {
        if (memberNode->heartbeat - TREMOVE >= it->gettimestamp()) {
            addrtoberemoved = createAddressFromIdPort(it->id, it->port);
            log->logNodeRemove(&memberNode->addr, &addrtoberemoved);
            it = memberNode->memberList.erase(it);
        } else 
            ++it;
    }
}

void MP1Node::printNodes() {
#ifdef DEBUGLOG
    Address addr;
    static char s[200];
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    if (it->id != getIdFromAddress(&memberNode->addr)) 
        printf("PROBLEM\n");
    while (it != memberNode->memberList.end()) {
        addr = createAddressFromIdPort(it->id, it->port);
        if (memberNode->heartbeat - TFAIL >= it->gettimestamp()) 
            sprintf(s, "%d:%d failed. (HB: %ld, TS: %ld)", it->id, it->port, it->getheartbeat(), it->gettimestamp());
        else
            sprintf(s, "%d:%d alive. (HB: %ld, TS: %ld)", it->id, it->port, it->getheartbeat(), it->gettimestamp());
        log->LOG(&memberNode->addr, s);
        ++it;
    }
#endif
}

void MP1Node::sendPing() {
    int offset = 0;
    int size = memberNode->memberList.size();
    int pos = (memberNode->heartbeat + offset) % (size-1) + 1;  // round robin pinging. Entry 0 is always me   // Random pinging //random((u_long)1, memberNode->memberList.size() - 1);  
    while (offset < size  && memberNode->memberList.at(pos).gettimestamp() < memberNode->heartbeat - TFAIL)  // We selected a failed node AND haven't exceeded retries
        pos = (memberNode->heartbeat + ++offset) % (size-1) + 1;
    if (memberNode->memberList.at(pos).gettimestamp() > memberNode->heartbeat - TFAIL)  // Ping only not failed nodes
        sendPing(memberNode->memberList.at(pos).getid(), memberNode->memberList.at(pos).getport());
}

void MP1Node::sendPing(int id, short port) {
    sendMessage(PINGREQ, id, port);
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    //memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    MemberListEntry *myentry = (MemberListEntry *) malloc(sizeof(MemberListEntry) + 1);

    //memcpy(&port, &memberNode->addr.addr[4], sizeof(short));
    myentry->setid(getIdFromAddress(&memberNode->addr));
    myentry->setport(getPortFromAddress(&memberNode->addr));
    myentry->setheartbeat(memberNode->heartbeat);
    memberNode->memberList.push_back(*myentry);
    free(myentry);
    memberNode->myPos = memberNode->memberList.begin();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d ",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

int MP1Node::getIdFromAddress(Address *addr) {
    int ret;
    memcpy(&ret, &addr->addr[0], sizeof(int));
    return (ret);
}

short MP1Node::getPortFromAddress(Address *addr) {
    short ret;
    memcpy(&ret, &addr->addr[4], sizeof(short));
    return (ret);
}