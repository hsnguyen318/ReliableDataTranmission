# Name: Hoang Son Nguyen
# RDT Layer Project - CS 372 - OSU Fall 2023


from segment import Segment
import math


# #################################################################################################################### #
# RDTLayer                                                                                                             #
#                                                                                                                      #
# Description:                                                                                                         #
# The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable         #
# channel.                                                                                                             #
#                                                                                                                      #
#                                                                                                                      #
# Notes:                                                                                                               #
# This file is meant to be changed.                                                                                    #
#                                                                                                                      #
#                                                                                                                      #
# #################################################################################################################### #


class RDTLayer(object):
    # ################################################################################################################ #
    # Class Scope Variables                                                                                            #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    DATA_LENGTH = 4  # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15  # in characters          # Receive window size for flow-control
    sendChannel = None
    receiveChannel = None
    dataToSend = ''
    currentIteration = 0  # Use this for segment 'timeouts'
    # Add items as needed
    SEGMENT_PER_WINDOW = math.ceil(FLOW_CONTROL_WIN_SIZE / DATA_LENGTH)
    currSeqNum = 0
    currWindow = [0, DATA_LENGTH]
    nextACKNum = DATA_LENGTH
    pipeline = []

    # ################################################################################################################ #
    # __init__()                                                                                                       #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        # Add items as needed
        self.slicedDataList = []
        self.timeout = 0
        self.countSegmentTimeouts = 0
        self.clientState = 0
        self.currACKNum = 0
        self.seqNum = 0

    # ################################################################################################################ #
    # setSendChannel()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable sending lower-layer channel                                                 #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setSendChannel(self, channel):
        """Set send channel."""
        self.sendChannel = channel

    # ################################################################################################################ #
    # setReceiveChannel()                                                                                              #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable receiving lower-layer channel                                               #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setReceiveChannel(self, channel):
        """Set receive channel."""
        self.receiveChannel = channel

    # ################################################################################################################ #
    # setDataToSend()                                                                                                  #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the string data to send                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setDataToSend(self, data):
        """Set data to send and slice into list of length of data length."""
        self.dataToSend = data
        self.slicedDataList = [self.dataToSend[i:i + self.DATA_LENGTH] for i in range(0, len(self.dataToSend), 4)]

    # ################################################################################################################ #
    # getDataReceived()                                                                                                #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to get the currently received and buffered string data, in order                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def getDataReceived(self):
        """Function to sort data in the pipeline and build string."""
        # ############################################################################################################ #
        # Identify the data that has been received and sort each segment by segment number.
        sortedPipeline = sorted(self.pipeline)
        sortedData = ""
        for i in range(len(sortedPipeline)):
            sortedData += sortedPipeline[i][1]
        # ############################################################################################################ #
        return sortedData

    # ################################################################################################################ #
    # processData()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # "timeslice". Called by main once per iteration                                                                   #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processData(self):
        """Function to process data of the RDT layer."""
        # at each turn, we increment iteration, process send data and process receiveQueue
        self.currentIteration += 1
        self.processSend()
        self.processReceiveAndSendRespond()

    # ################################################################################################################ #
    # processSend()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment sending tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processSend(self):
        """Function to process data to be sent by slicing it into 4-char segment.
        Also contains logic of what to do when there is items in the receiving queue."""

        # clientState is defaulted to 0. If there is dataToSend, switch to 1 meaning RDT Layer is acting as Client.
        if self.dataToSend:
            self.clientState = 1

        # if there are segment ACKs in receive queue, process the ACKs in the receiverQueue of the Client
        if self.receiveChannel.receiveQueue:
            if self.clientState:
                receiveQueue = self.receiveChannel.receive()
                # for each segment in the receiveQueue, if the acknum is equal to the next acknum, meaning the
                # window has been fully processed then we increment everything by 4 to move to the next window
                for i in range(len(receiveQueue)):
                    if receiveQueue[i].acknum == self.nextACKNum:
                        self.currSeqNum += self.SEGMENT_PER_WINDOW
                        self.nextACKNum += self.SEGMENT_PER_WINDOW
                        self.currWindow[0] += self.SEGMENT_PER_WINDOW
                        self.currWindow[1] += self.SEGMENT_PER_WINDOW
        # Else if receiveQueue is empty, there is no segment to process. The RDT layer will wait for timeout.
        else:
            # if a few iterations have passed, we increment count of timeouts for printing data at the end
            if self.timeout == 5:
                self.countSegmentTimeouts += 1
            # else, we skip iteration and increment timeout.
            else:
                self.timeout += 1  # 5 timeouts have not passed, so we just increment it to skip this iteration
                return

        # if RDT layer is client, send current segment data. Server does not send data segments.
        if self.clientState:
            self.sendSegment(self.currSeqNum, self.slicedDataList)

    # ################################################################################################################ #
    # sendSegment()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Send segments of data packets                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #

    def sendSegment(self, seqNum, slicedDataList):
        """Function to send segment over the channel."""
        # sending each segment as long as there is data to send, and segNum < number of slices
        for i in range(int(self.SEGMENT_PER_WINDOW)):
            if self.dataToSend and seqNum < len(slicedDataList):
                # Create segment object and set data
                segmentSend = Segment()
                segmentSend.setData(seqNum, slicedDataList[seqNum])
                # Use the unreliable sendChannel to send the segment
                print("Sending segment: ", segmentSend.to_string())
                self.sendChannel.send(segmentSend)
                # increment segNum to keep count
                seqNum += 1

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #

    def processReceiveAndSendRespond(self):
        """Function to process segment ACKs."""

        # This call returns a list of incoming segments (see Segment class)...
        listIncomingSegments = self.receiveChannel.receive()

        segmentToBeProcessedList = []  # list to store incoming segments to be processed

        # If there are segments in the receiveChannel, we must process it by:
        if listIncomingSegments:

            # 1. Determine which segments to be selected for further processing. To avoid duplicated segments, we
            # only select segments that (1) contain data, (2) has valid checksum, (3) not duplicated with any segment
            # in the existing list and (4) has to be in the current window
            # if a segment passes all the criteria, its seqnum and payload are added to the list of segments to be
            # processed further.
            for incomingSegment in listIncomingSegments:
                containData = incomingSegment.payload is not None
                validCheckSum = incomingSegment.checkChecksum() is True
                nondup = incomingSegment not in segmentToBeProcessedList
                inbound = self.currWindow[0] <= incomingSegment.seqnum <= self.currWindow[1]
                if containData and validCheckSum and nondup and inbound:
                    segmentToBeProcessedList.append([incomingSegment.seqnum, incomingSegment.payload])

            # 2. For segments added to this list, if it's not already in the pipeline to be sorted, we add it so
            # there will not be any duplicated segments.
            for incomingSegment in segmentToBeProcessedList:
                if incomingSegment not in self.pipeline:
                    self.pipeline.append(incomingSegment)

            # 3. If len of the segmentToBeProcessedList is 4, it means we have processed all segments in this
            # window, so we move to the next window by incrementing current ack number
            # we also send ack segments
            if len(segmentToBeProcessedList) == 4:
                self.currACKNum += 4
                segmentAck = Segment()
                segmentAck.setAck(self.currWindow[0] + 4)
                print("Sending ack: ", segmentAck.to_string())
                self.sendChannel.send(segmentAck)
