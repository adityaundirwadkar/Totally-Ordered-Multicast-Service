//Import Section
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

public class Project1 {
	//Global Variables
	public static ArrayList<String[]> listOfNodes = new ArrayList<String[]>();
    //listOfNodes{nodeID,nodeIPAddress,portNumber,Status}
	//To retrieve rowList.get(1)[1] -- Second Row & Second Element
	
	public static ArrayList<String[]> sentMessages = new ArrayList<String[]>();
	//Store the sent messages with their received ACK messages {Message, actualClients,noOfACKs}
	//message string will be of type: {actualMessage,LCValue,nodeID,messageType,timestampOfMessage}
	
	public static ArrayList<String[]> receivedMessages = new ArrayList<String[]>();
	//Store the newly received messages until their actual value of lamport's logical clock value arrives.
	//receivedMessage string will be of type: {actualMessage,LCValue,nodeID,messageType,timestampOfMessage,sentACK}
	
	public static ArrayList<String[]> deliverableMessages = new ArrayList<String[]>();
	//Messages that are to be delivered after receiving a isFinal message
	// deliverableMessages = {message, senderID, LCclockValue}
	
	public static ArrayList<String[]> sendReceiveQueue = new ArrayList<String[]>();
	//sendReceiveQueue will buffer the messages until all the ACK has been received and/or isFinal message has been received.
	//sendReceiveQueue = {send/receive, completeMessage}
	
	public static int bytebufferCapacity = 100;
	public static int nodeID;
	public static String hostName;
	public static int localPortNumber;
	public static String configFilePath;
	public static int lamportClockValue=0;
	
	public static void main(String[] args){
		//java SystemClass nodeID ClientIDs
		Project1 project1;
		//args[0] nodeID
		try {
			project1 = new Project1(args[0]);
			int loopVariable=0;
			Random newRandom = new Random();
			boolean flagForTermination = true;
			String[] newReceivedMessage;
			
			//Now retrieve the value of clients and number of messages to be sent.
			
			String clientMessageInformation = project1.getClientAndMessagesInfo(args[0]);
			String[] splitClient = clientMessageInformation.split(",");
			//splitClient[0] -> clients , splitClient[1] -> number of messages
			
			int noOfMessages = Integer.parseInt(splitClient[1]);
			
			int someNumber = nodeID * 1000;
			Thread.sleep(4000 + someNumber);
			//Terminate only when all the nodes are terminated...
			while((!project1.areAllTerminated())){
				//Send the message...
				Thread.sleep(7000);
				if(loopVariable < noOfMessages){
					project1.m_send(String.valueOf(newRandom.nextInt(99)), splitClient[0]);
					loopVariable++;
				}
				
				//Read from the queue...
				
				newReceivedMessage = project1.m_receive();
				if(!newReceivedMessage[0].equalsIgnoreCase("No new Messages")){
					System.out.println("----------------------------------------------");
					System.out.println("| Message: "+newReceivedMessage[0]+ " | Sender : " +newReceivedMessage[1]+ " | ClockValue : "+newReceivedMessage[2]+" |");
					System.out.println("----------------------------------------------");
				}
				
				if(project1.isDeliverablesEmpty() && loopVariable >= noOfMessages && flagForTermination){
					System.out.println("Sleeping for 30 seconds!!!!");
					Thread.sleep(30000);
					System.out.println("Sending termination signal!!");
					//project1.printNodeInfo();
					project1.m_send_termination_signal();
					Thread.sleep(5000);
					//project1.printNodeInfo();
					flagForTermination = false;
				}
				
			}
			System.out.println("Shutting down the node in... ");
			loopVariable=5;
			while (loopVariable > 0){
				Thread.sleep(1000);
				System.out.print(+loopVariable+ " ");
				loopVariable--;
			}
			System.out.println();
			
			/*while((loopVariable < noOfMessages)){
				//Code to send messages
				Thread.sleep(7000);
				newSystemClass.m_send(String.valueOf(newRandom.nextInt(99)), args[1]);
				loopVariable++;
			}

			//Code to view Received messages
			Thread.sleep(30000);
			int countdownTimer = 5000;
			System.out.println("Reading messages in..");
			while (countdownTimer > 0){
				Thread.sleep(1000);
				System.out.println(+countdownTimer/1000);
				countdownTimer= countdownTimer - 1000;
			}
			
			System.out.println("----------------------------------------------");
			String[] newReceivedMessage = newSystemClass.m_receive();
			while(!newReceivedMessage[0].equalsIgnoreCase("No new Messages")){				
				System.out.println("| Message: "+newReceivedMessage[0]+ " | Sender : " +newReceivedMessage[1]+ " | ClockValue : "+newReceivedMessage[2]+" |");
				Thread.sleep(2000);
				newReceivedMessage = newSystemClass.m_receive();
			}
			
			System.out.println("----------------------------------------------");
			//Send the termination signal!
			System.out.println("Sending Termination message for Node: " +args[0]);
			countdownTimer=5000;
			while (countdownTimer > 0){
				Thread.sleep(1000);
				System.out.println(+countdownTimer/1000);
				countdownTimer= countdownTimer - 1000;
			}
			Thread.sleep(10000);
			newSystemClass.m_send_termination_signal();
			newSystemClass.printNodeInfo();*/
			
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}
	
    //Override Constructor to initialize global variables.
	public Project1(String nodeid) throws InterruptedException{
		try{
			//Thread.sleep(5000);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		Project1.nodeID = Integer.parseInt(nodeid);				//Assign ID value to system
    	//SystemClass.configFilePath = filepath;						//Assign path for configuration file
    	//SystemClass.hostName = hostaddress;							//Assign local host name
    	//SystemClass.localPortNumber = Integer.parseInt(localport);	//Assign portNumber
    	
    	

		
		String currentLine = null;
        String configFileName = "config.txt";		//Configuration file name
        String configFileTerminator = "[END NODE DETAILS]";	//Terminating character
        boolean terminateConfigFileLoop = false;
        BufferedReader configFileReader = null;
        
        //Import network information from a configuration File into listOfNodes.
        try{
        	configFileReader = new BufferedReader(new FileReader("./" +configFileName));
        	//configFileReader = new BufferedReader(new FileReader(configFilePath));
        	configFileReader.readLine();
        	while(!terminateConfigFileLoop){
        		currentLine = configFileReader.readLine();
        		if(currentLine.equalsIgnoreCase(configFileTerminator)){
        			terminateConfigFileLoop=true;
        			break;
        		}
        		String[] splitRemoteHostDetails = currentLine.split(",");
        		listOfNodes.add(new String[] {splitRemoteHostDetails[0],splitRemoteHostDetails[1],splitRemoteHostDetails[2],"ALIVE"});
        		//System.out.println(currentLine);
        		
        	}
        	configFileReader.close();
        }
        catch (Exception e){
        	System.out.println("Unable to open the config file \nReason: " +e.getCause());
        }
        finally{
        	try {
				if (configFileReader != null)
					configFileReader.close();
			} 
        	catch (IOException ex) {
				ex.printStackTrace();
			}
        }
        
        //Get Node hostName & portNumber
        String[] tempNodeInfo = getNodeInfo(nodeid);
        Project1.hostName = tempNodeInfo[1];								//Assign local host name
        Project1.localPortNumber = Integer.parseInt(tempNodeInfo[2]);	//Assign portNumber
        
    	//Initialize a receiveServer thread
    	Thread newReceiver = new recieveServer();
    	newReceiver.start();
        
	}
	
	public String getClientAndMessagesInfo(String nodeID){
		String currentLine = null;
        String configFileName = "clientMessageInfo.txt";		//Configuration file name
        String configFileTerminator = "[END]";	//Terminating character
        boolean terminateConfigFileLoop = false;
        BufferedReader configFileReader = null;
      //Import network information from a configuration File into listOfNodes.
        try{
        	configFileReader = new BufferedReader(new FileReader("./" +configFileName));
        	//configFileReader = new BufferedReader(new FileReader(configFilePath));
        	configFileReader.readLine();
        	while(!terminateConfigFileLoop){
        		currentLine = configFileReader.readLine();
        		if(currentLine.equalsIgnoreCase(configFileTerminator)){
        			terminateConfigFileLoop=true;
        			break;
        		}
        		String[] splitRemoteHostDetails = currentLine.split(",");
        		//System.out.println(currentLine);
        		if(splitRemoteHostDetails[0].equalsIgnoreCase(nodeID)){
        			return (splitRemoteHostDetails[1]+","+splitRemoteHostDetails[2]);
        		}        		
        	}
        	configFileReader.close();
        }
        catch (Exception e){
        	System.out.println("Unable to open the config file \nReason: " +e.getCause());
        }
        finally{
        	try {
				if (configFileReader != null)
					configFileReader.close();
			} 
        	catch (IOException ex) {
				ex.printStackTrace();
			}
        }
		return null;		
	}
	
	public synchronized void m_send_termination_signal(){
		long currentTimeStamp = System.currentTimeMillis();
		String newlamportClockValue = String.valueOf(getLamportsClockCurrentValue());
		//BroadCast this message, so no specific recipients and no message!
		Thread newSendMessage = new sendServer("isTerm", null , newlamportClockValue, String.valueOf(currentTimeStamp), null);
		newSendMessage.start();
	}
	
	public synchronized void m_send(String multicastMessage, String receipients){
		long currentTimeStamp = System.currentTimeMillis();
		String newlamportClockValue = String.valueOf(getLamportsClockCurrentValue());
		Thread newSendMessage = new sendServer("isFirst", multicastMessage , newlamportClockValue, String.valueOf(currentTimeStamp), receipients);
		newSendMessage.start();
	}
	
	public synchronized String[] m_receive(){
		String[] newlyReceivedMessage = {"No new Messages", null, null};
		//System.out.println("deliverableMessages.size()" +deliverableMessages.size());
		if(deliverableMessages.size() > 0){
			newlyReceivedMessage = deliverableMessages.get(0);
			deliverableMessages.remove(0);
			//return newlyReceivedMessage;
		}
		return newlyReceivedMessage;
	}
	
	//Access global variables in synchronized way

	public synchronized int getNoOfNodes(){
		return listOfNodes.size();
	}
	
	public synchronized String[] getNodeInfo(String nodeid){
		return listOfNodes.get(Integer.parseInt(nodeid));
	}
	
	public synchronized void changeNodeStatus(int clientID ,String clientIPAddress, String clientPortNumber, String Status){
		String[] tempString= {String.valueOf(clientID),clientIPAddress,clientPortNumber,Status };
		listOfNodes.set(clientID,tempString);
	}
	
	public synchronized int getLamportsClockCurrentValue(){
		return (lamportClockValue);
	}
	
	public synchronized int getLamportsClockNextValue(){
		//LamportClock value will be checked only when receive and send event happen. So increment the value on every operation.
		return (++lamportClockValue);
	}

	public synchronized void setLamportsClockValue(int newLamportClockValue){
		lamportClockValue= newLamportClockValue;
	}

	public synchronized int addNewSentMessage(String newmessage, String clients){
		String[] newMessage = {newmessage, clients, "0"};
		sentMessages.add(newMessage);
		return sentMessages.size();
	}
	
	public synchronized String[] getSentMessageInfo(int messagePosition){
		return sentMessages.get(messagePosition);		
	}
	
	public synchronized void setSentMessageInfo(int messagePosition, String[] newMessage){
		sentMessages.set(messagePosition,newMessage);
	}
	
	public synchronized int addNewReceivedMessage(String actualMessage, String lamportsClockValue, String senderID, String messageType, String timeStamp, String ackSent){
		String[] newReceivedMessage = {actualMessage,lamportsClockValue,senderID,messageType,timeStamp,ackSent};
		receivedMessages.add(newReceivedMessage);
		//System.out.println("Added new message in receivedMessages"+actualMessage+ " & size is now : " +receivedMessages.size());
		return receivedMessages.size();
		
	}
	
	public synchronized void setReceivedMessageAttributes(int receievdMessagePosition, String actualmessage, String lamportsClockVal, String senderid, String messagetype, String timestamp, String acktoken){
		String[] tempRecievedMessage = {actualmessage, lamportsClockVal, senderid , messagetype, timestamp, acktoken};
		receivedMessages.set(receievdMessagePosition, tempRecievedMessage);
	}
	
	public synchronized boolean areAllSentMessagesDelivered(){
		boolean delivered = true;
		int totalSentMessages = sentMessages.size();
		//Traverse in bottom-up approach for faster Search.
		while(totalSentMessages > 0 && delivered){
			if(Integer.parseInt(sentMessages.get(totalSentMessages-1)[2]) > 0)
				delivered = false;
			totalSentMessages--;
		}
		return delivered;
	}
	
	public synchronized boolean areAllReceivedMessagesisFinal(){
		boolean isFinal = true;
		int totalReceivedMessages = receivedMessages.size();
		//Traverse in bottom-up approach
		while(totalReceivedMessages > 0 && isFinal){
			if(!receivedMessages.get(totalReceivedMessages-1)[3].equalsIgnoreCase("isFinal")){
				isFinal = false;
			}
			totalReceivedMessages--;
		}
		return isFinal;
	}
	
	public synchronized boolean isSendReceiveQueueEmpty(){
		if(sendReceiveQueue.size() > 0)
			return false;
		else
			return true;
	}
	
	public synchronized void addToSendReceiveQueue(String sendReceive, String actualmessage, String lamportclockvalue, String clientsenderid, String messagetype, String timestampvalue, String acksent, String clients){
    	//dividedMessage[0] = actualMessage
    	//dividedMessage[1] = clientLCValue
    	//dividedMessage[2] = senderID
		//dividedMessage[3] = messageType
    	//dividedMessage[4] = timestampOfMessage
    	
		if(sendReceive.equalsIgnoreCase("Received")){
			String newReceiveMessage = actualmessage+","+lamportclockvalue+","+clientsenderid+","+messagetype+","+timestampvalue+","+acksent;
			String[] newRow = {sendReceive,newReceiveMessage};
			sendReceiveQueue.add(newRow);
			//System.out.println("Added New Row in sendReceiveQueue which has size: "+sendReceiveQueue.size());
		}
		else if(sendReceive.equalsIgnoreCase("Send")){
			//completemessage#actualClients#noOfACKs
			String newSendMessage = actualmessage+","+lamportclockvalue+","+clientsenderid+","+messagetype+","+timestampvalue
									+"#"+clients
									+"#"+"0";
			//sendMessage will be stored as completeMessage#clientIDs#receivedACKS
			String[] newRow = {sendReceive,newSendMessage};
			sendReceiveQueue.add(newRow);
			//System.out.println("newSendMessage : "+newSendMessage);
			//System.out.println("Added New Row in sendReceiveQueue which has size: "+sendReceiveQueue.size());
		}		
	}
	
	public synchronized void addToDeliverables(String message, String senderid, String lamportclockvalue){
		String[] newDeliverableMessage = {message, senderid, lamportclockvalue};
		deliverableMessages.add(newDeliverableMessage);
	}
	
	public synchronized String[] getCompleteSentMessageInfo(String actualmessage, String timestampofmessage){
		//Look into sentMessages
		int totalSentMessages = sentMessages.size();
		String[] tempsentmessage, splittempsentmessage;
		 
		while (totalSentMessages > 0){
			tempsentmessage = sentMessages.get(totalSentMessages-1);
			splittempsentmessage = tempsentmessage[0].split(",");
			//System.out.println("Looking for message!");
			//System.out.println(tempsentmessage[1]+","+tempsentmessage[2]);
			
			//System.out.println(splittempsentmessage[0]+","+Long.parseLong(splittempsentmessage[4]));
			
			//message string will be of type: {actualMessage,LCValue,nodeID,messageType,timestampOfMessage}
			//if(splittempsentmessage[0].equalsIgnoreCase(actualmessage)){
			if(splittempsentmessage[0].equalsIgnoreCase(actualmessage) && splittempsentmessage[3].equalsIgnoreCase("isFirst")){
				//System.out.println("Found message!!");
				String[] newmessage = {String.valueOf(totalSentMessages-1), tempsentmessage[0],tempsentmessage[1],tempsentmessage[2]};
				//send newmessage = {messagePosition, message, NoOfClients, NoOfACKs}
				return newmessage;
			}
			totalSentMessages--;
		}
		return null;
	}
	
	public synchronized String[] getCompleteReceivedMessageInfo(String actualmessage, String timestampofmessage){
		int totalReceivedMessages = receivedMessages.size();
		String[] tempReceivedMessage = null;
		while(totalReceivedMessages > 0){
			//System.out.println("Looking for a received message with actualmessage:" +actualmessage+ " & timestampofmessage: "+timestampofmessage);
			//actualMessage,LCValue,nodeID,messageType,timestampOfMessage,sentACK
			tempReceivedMessage = receivedMessages.get(totalReceivedMessages -1);
			//System.out.println("actualmessage: "+tempReceivedMessage[0]+" timestampOfMessage: "+tempReceivedMessage[4]);
			if(tempReceivedMessage[0].equalsIgnoreCase(actualmessage) && tempReceivedMessage[4].equalsIgnoreCase(timestampofmessage)){
				//System.out.println("Yes found message!");
				String[] receivedmessage = {String.valueOf(totalReceivedMessages -1), tempReceivedMessage[0], tempReceivedMessage[1], tempReceivedMessage[2], tempReceivedMessage[3], tempReceivedMessage[4], tempReceivedMessage[5]};
				return receivedmessage;
			}	
			totalReceivedMessages--;
		}
		return null;
	}
	
	public synchronized void checkSendReceiveQueue() throws InterruptedException{
		int currentLamportClockVal = 0;
		//Check whether sendReceiveQueue is empty, proceed only if not
		if(sendReceiveQueue.size() > 0){
			String[] firstElementOfSendReceiveQueue = sendReceiveQueue.get(0);
			//Check what message is buffered!
			if(firstElementOfSendReceiveQueue[0].equalsIgnoreCase("Received")){
				//for a received buffered message
				
				//Increment the Lamport clock value
				currentLamportClockVal = getLamportsClockNextValue();
				
				//Extract the message
				//actualmessage+","+lamportclockvalue+","+clientsenderid+","+messagetype+","+timestampvalue+","+acksent
				String[] receivedMessage =  firstElementOfSendReceiveQueue[1].split(",");
				
				//Compare the Lamport Clock Value
				if(currentLamportClockVal < Integer.parseInt(receivedMessage[1])){
					currentLamportClockVal = Integer.parseInt(receivedMessage[1]);
				}
				else {
					receivedMessage[1] = String.valueOf(currentLamportClockVal);
				}
				
				//Set system's Lamport Clock
				setLamportsClockValue(currentLamportClockVal);
				
				//Add new message to receivedMessages with sentACK =YES
				//actualmessage+","+lamportclockvalue+","+clientsenderid+","+messagetype+","+timestampvalue+","+acksent;
				//addNewReceivedMessage(String actualMessage, String lamportsClockValue, String senderID, String messageType, String timeStamp, String ackSent)
				addNewReceivedMessage(receivedMessage[0], String.valueOf(currentLamportClockVal), receivedMessage[2], receivedMessage[3], receivedMessage[4], "Yes");
				
				//Now send the ACK 
				//sendServer(String messageType, String message, String lamportClock, String currentTimeStamp, String clients)
				
				//sendServer(String messageType, String message, String lamportClock, String currentTimeStamp, String clients)
    			//Thread newSendACK = new sendServer("isACK",dividedMessage[0], String.valueOf(currentLamportsClockValue), dividedMessage[4], dividedMessage[2]);
    			
				Thread newSendServerACK = new sendServer("isACK",receivedMessage[0],String.valueOf(currentLamportClockVal),receivedMessage[4], receivedMessage[2]);
				//System.out.println("Sending an ACK message to buffered message to client: " +receivedMessage[2]);
				Thread.sleep(1000);
				newSendServerACK.start();
			}
			if(firstElementOfSendReceiveQueue[0].equalsIgnoreCase("Send")){
				//For send message.
				//System.out.println("Sending first buffered Message!!");
				//Extract the message
				//completeMessage#Clients#NoOfACKs
				String[] sendMessage = firstElementOfSendReceiveQueue[1].split("#");
				
				//System.out.println("sendMessage[0]: "+sendMessage[0]);
				//System.out.println("sendMessage[1]: "+sendMessage[1]);
				//System.out.println("sendMessage[2]: "+sendMessage[2]);
				
				
				//Now get the atomic message
				//actualmessage+","+lamportclockvalue+","+clientsenderid+","+messagetype+","+timestampvalue
				String[] atomicSendMessage = sendMessage[0].split(",");
				
				//Assign Lamport clock value
				//Increment the Lamport clock value
				currentLamportClockVal = getLamportsClockNextValue();
				
				//send it to the clients.
				//sendServer(String messageType, String message, String lamportClock, String currentTimeStamp, String clients)
				Thread newSendMessage = new sendServer("isBuffered", atomicSendMessage[0],String.valueOf(currentLamportClockVal), atomicSendMessage[4], sendMessage[1]);
				//System.out.println("Sending buffered message to :" +sendMessage[1]);
				newSendMessage.start();
			}	
			//Pop the first Send/Receive message from the queue.
			//System.out.println("Removed first message from sendReceiveQueue...");
			sendReceiveQueue.remove(0);
		}		
	}

	public synchronized boolean areAllTerminated(){
		//This function returns true if all nodes are terminated
		int loopVariable = 0, totalNodes = listOfNodes.size();
		while(loopVariable < totalNodes){
			String[] tempNodeInfo = listOfNodes.get(loopVariable);
			if(tempNodeInfo[3].equalsIgnoreCase("Alive")){
				//Return false if any one of them has a status as ALIVE!
				return false;
			}
			loopVariable++;
		}
		//Return true if all of them have status as DEAD or TERMINATED!
		return true;
	}

	public synchronized boolean isDeliverablesEmpty()
	{
		if(deliverableMessages.size() > 0){
			return false;
		}
		else{
			return true;
		}
	}
	
	public synchronized void printNodeInfo(){
		int loopVariable = 0 , numberOfNodes = listOfNodes.size();
		String[] tempNodeInfo;
		while(loopVariable < numberOfNodes){
			tempNodeInfo = listOfNodes.get(loopVariable);
			System.out.println(tempNodeInfo[0]+", "+tempNodeInfo[1]+", "+tempNodeInfo[2]+", "+tempNodeInfo[3]);
			loopVariable++;
		}
	}
	
	public class recieveServer extends Thread{
		
		//Receive Events
		public void executeReceiveServer(){
			//Server Variables 
			SctpServerChannel sctpServerChannelForServer;
	        SocketAddress socketAddressForServer;
	        SctpChannel sctpChannelForServer;
	        //ByteBuffer byteBufferForServer;
	        @SuppressWarnings("unused")
			MessageInfo messageInfoForServer;
	        String[] dividedMessage = null, tempSentMessage = null, tempReceivedMessage = null;
	        int currentLamportsClockValue;
	        
	        //Run the server until all the nodes terminate.
	        try{
	        	socketAddressForServer = new InetSocketAddress(hostName,localPortNumber);
	        	//byteBufferForServer = ByteBuffer.allocate(bytebufferCapacity);
	        	sctpServerChannelForServer = SctpServerChannel.open();
	        	//System.out.println("Create and bind for SCTP Address.");
	        	sctpServerChannelForServer.bind(socketAddressForServer);
		        System.out.println(hostName+ " is listening on Port No: " +localPortNumber);
		        while (!areAllTerminated()) {
		        //while ((sctpChannelForServer = sctpServerChannelForServer.accept()) != null && !areAllTerminated()) {
		        	sctpChannelForServer = sctpServerChannelForServer.accept();
		        	final ByteBuffer byteBufferForServer = ByteBuffer.allocate(bytebufferCapacity);
		        	//System.out.println("Client Connection received..");
		        	messageInfoForServer = sctpChannelForServer.receive(byteBufferForServer , null, null);
		        	//dividedMessage = splitMessage(byteToString(byteBufferForServer));
		        	
		        	
		        	//dividedMessage[0] = actualMessage
		        	//dividedMessage[1] = clientLCValue
		        	//dividedMessage[2] = senderID
		        	//dividedMessage[3] = messageType
		        	//dividedMessage[4] = timestampOfMessage
		        	dividedMessage = (byteToString(byteBufferForServer)).split(",");
        	
		        	//System.out.println("actualMessage: "+dividedMessage[0]);
		        	//System.out.println("clientLCValue: "+dividedMessage[1]);
		        	//System.out.println("senderID: "+Integer.parseInt(dividedMessage[2]));
		        	//System.out.println("messageType: "+dividedMessage[3]);
		        	//System.out.println("timestampOfMessage: "+dividedMessage[4]);

		        	//Four Receive Events
		        	//1. isFirst
		        	if(dividedMessage[3].equalsIgnoreCase("isFirst")){
		        		//System.out.println("Message received isFirst");
		        		//Check all sentMessages are delivered, receivedMessages have isFinal and sendReceiveQueue is empty
		        		if((areAllSentMessagesDelivered() || nodeID > Integer.parseInt(dividedMessage[2]))  && isSendReceiveQueueEmpty()){
		        		//if((areAllSentMessagesDelivered() || nodeID > Integer.parseInt(dividedMessage[2])) && areAllReceivedMessagesisFinal() && isSendReceiveQueueEmpty()){
		        			//System.out.println("Im in isFirst if..!!");
		        			//Get Systems' Lamport's clock value.
		        			currentLamportsClockValue = getLamportsClockNextValue();
		        			//Compares LC value with Sender's clock value
		        			currentLamportsClockValue = Math.max(currentLamportsClockValue,Integer.parseInt(dividedMessage[1]));
		        			
		        			//Set the Systems' LC value 
		        			setLamportsClockValue(currentLamportsClockValue);
		        			
		        			//Put it in receivedMessages and mark it as sentACK
		        			addNewReceivedMessage(dividedMessage[0], String.valueOf(currentLamportsClockValue), dividedMessage[2], dividedMessage[3], dividedMessage[4], "Yes");
		        			
		        			//Now send the ACK
		        			//sendServer(String messageType, String message, String lamportClock, String currentTimeStamp, String clients)
		        			Thread newSendACK = new sendServer("isACK",dividedMessage[0], String.valueOf(currentLamportsClockValue), dividedMessage[4], dividedMessage[2]);
		        			
		        			//System.out.println("isACK,"+dividedMessage[0]+","+ String.valueOf(currentLamportsClockValue)+","+ dividedMessage[4]+","+Integer.parseInt(dividedMessage[2]));
		        			//System.out.println("Now sending an isACK message");
		        			Thread.sleep(1000);
		        			newSendACK.start();
		        		}
		        		else{
		        			//System.out.println("Im in isFirst else..!!");
		        			//If not simple, add it in sendReceiveQueue and delay its ACK
		        			addToSendReceiveQueue("Received", dividedMessage[0], dividedMessage[1], dividedMessage[2], dividedMessage[3], dividedMessage[4], "No", null);
		        		}
		        	}

		        	//2. isFinal
		        	//dividedMessage[0] = actualMessage
		        	//dividedMessage[1] = clientLCValue
		        	//dividedMessage[2] = senderID
		        	//dividedMessage[3] = messageType
		        	//dividedMessage[4] = timestampOfMessage
					if(dividedMessage[3].equalsIgnoreCase("isFinal")){
						//System.out.println("Message received isFinal");
						//Check for LC value with systems' LC value
						//currentLamportsClockValue = getLamportsClockCurrentValue();
						//currentLamportsClockValue = Math.max(currentLamportsClockValue, Integer.parseInt(dividedMessage[1]));
						
						//Set it accordingly
						//setLamportsClockValue(currentLamportsClockValue);
						
						//Retrieve the old message
						tempReceivedMessage = getCompleteReceivedMessageInfo(dividedMessage[0], dividedMessage[4]);
						//actualMessage,LCValue,nodeID,messageType,timestampOfMessage,sentACK
						
						//System.out.println("tempReceivedMessage[1]: "+tempReceivedMessage[1]);
						//System.out.println("tempReceivedMessage[2]: "+tempReceivedMessage[2]);
						//System.out.println("tempReceivedMessage[3]: "+tempReceivedMessage[3]);
						//System.out.println("tempReceivedMessage[4]: "+tempReceivedMessage[4]);
						//System.out.println("tempReceivedMessage[5]: "+tempReceivedMessage[5]);
						
						//change the value of messageType in receivedMessages
						currentLamportsClockValue = Math.max(Integer.parseInt(dividedMessage[1]),Integer.parseInt(tempReceivedMessage[2]));
						//Set it accordingly
						setLamportsClockValue(currentLamportsClockValue);
						
						//receivedMessagePosition,actualMessage,LCValue,nodeID,messageType,timestampOfMessage,sentACK
						tempReceivedMessage[2] = String.valueOf(currentLamportsClockValue);
						tempReceivedMessage[4] = "isFinal";
						
						
						setReceivedMessageAttributes(Integer.parseInt(tempReceivedMessage[0]), tempReceivedMessage[1], tempReceivedMessage[2], tempReceivedMessage[3], tempReceivedMessage[4], tempReceivedMessage[5], tempReceivedMessage[6]);
						//System.out.println("Changed the value of the string...");
						
						
						//put it in deliverables.
						addToDeliverables(dividedMessage[0], dividedMessage[2], String.valueOf(currentLamportsClockValue));
						//System.out.println("Added to deliverables..");
						//check for sendReceiveQueue for next buffered message.
						checkSendReceiveQueue();
						
					}
					//3. isACK
		        	//dividedMessage[0] = actualMessage
		        	//dividedMessage[1] = clientLCValue
		        	//dividedMessage[2] = senderID
		        	//dividedMessage[3] = messageType
		        	//dividedMessage[4] = timestampOfMessage
					if(dividedMessage[3].equalsIgnoreCase("isACK")){
						//System.out.println("Message received isACK");
						//Retrieve entire row of sentMessages by comparing the timeStamp and value of the message
						tempSentMessage = getCompleteSentMessageInfo(dividedMessage[0], dividedMessage[4]);
						//{messagePosition, message, NoOfClients, NoOfACKs}						
						
						//Set the Lamport clock value accordingly
						currentLamportsClockValue = getLamportsClockCurrentValue();
						currentLamportsClockValue = Math.max(currentLamportsClockValue, Integer.parseInt(dividedMessage[1]));
						
						//System.out.println("currentLamportsClockValue: " +currentLamportsClockValue);
						
						//Proceed only if the message was really sent otherwise ignore it.
						if(tempSentMessage != null){
							
							//Split the message to get the received Lamport Clock value.
							String[] splittempSentMessage = tempSentMessage[1].split(",");
							//{actualMessage,LCValue,nodeID,messageType,timestampOfMessage}
							
							//Compare Lamport clock value.
							if(Integer.parseInt(splittempSentMessage[1]) > currentLamportsClockValue){
								currentLamportsClockValue = Integer.parseInt(splittempSentMessage[1]);
							}
							else{
								splittempSentMessage[1] = String.valueOf(currentLamportsClockValue);
							}
							
							//System.out.println("currentLamportsClockValue: " +currentLamportsClockValue);
							
							
							//Decrement the count of ACKs received
							tempSentMessage[3] = String.valueOf((Integer.parseInt(tempSentMessage[3]))-1);
							//System.out.println("count of ACKs received : " +(Integer.parseInt(tempSentMessage[3])));
							
							//is this one the last ACK?
							if(Integer.parseInt(tempSentMessage[3]) <= 0){
								//Prepare to send the isFinal Message
								
								//sendServer(String messageType, String message, String lamportClock, String currentTimeStamp, String clients)
								//System.out.println("Sending isFinal message!");
								Thread newClients = new sendServer("isFinal", splittempSentMessage[0],String.valueOf(currentLamportsClockValue), splittempSentMessage[4],tempSentMessage[2] );
								newClients.start();
								
								//Change systems Lamport's Clock value
								setLamportsClockValue(currentLamportsClockValue);
								
								//Change the message type to isFinal
								splittempSentMessage[3] = "isFinal";								
								
								//Now change the contents of sentMessages for this particular message.
								String[] modifySentMessage = {splittempSentMessage[0]+","+splittempSentMessage[1]+","+splittempSentMessage[2]+","+splittempSentMessage[3]+","+splittempSentMessage[4],
																tempSentMessage[2],
																tempSentMessage[3]
																};
								
								//Change the contents of sentMessage for this message
								setSentMessageInfo(Integer.parseInt(tempSentMessage[0]), modifySentMessage);
								
								//Check for any buffered messages.
								checkSendReceiveQueue();
							}
							else{
								//Decrement the count of ACKs received
								//tempSentMessage[3] = String.valueOf((Integer.parseInt(tempSentMessage[3]))-1);
								
								String[] modifySentMessage = {splittempSentMessage[0]+","+splittempSentMessage[1]+","+splittempSentMessage[2]+","+splittempSentMessage[3]+","+splittempSentMessage[4],
										tempSentMessage[2],
										tempSentMessage[3]
										};
								//Change the contents of sentMessage for this message
								setSentMessageInfo(Integer.parseInt(tempSentMessage[0]), modifySentMessage);
							}
						}
					}
					
					//4. isTerm
		        	//dividedMessage[0] = actualMessage
		        	//dividedMessage[1] = clientLCValue
		        	//dividedMessage[2] = senderID
		        	//dividedMessage[3] = messageType
		        	//dividedMessage[4] = timestampOfMessage
					if(dividedMessage[3].equalsIgnoreCase("isTerm")){
						//System.out.println("Received a termination request...");
						//listOfNodes{nodeID,nodeIPAddress,portNumber,Status}
						String[] tempGetNodeInfo = getNodeInfo(dividedMessage[2]);

						//changeNodeStatus(int clientID ,String clientIPAddress, String clientPortNumber, String Status)
						changeNodeStatus(Integer.parseInt(dividedMessage[2]) ,tempGetNodeInfo[1], tempGetNodeInfo[2], "TERMINATED");
						//Sleep for a second 
						Thread.sleep(1000);
						
						//System.out.println("Current System Status...");
						//printNodeInfo();
					}
		        }
		        System.out.println("Shutting Down the Receiver, Since all the nodes have become Inactive!");
	        }
	        catch (Exception e){
	        	System.out.println("Exception in executeServer.. \nReason: " +e.getCause());
	        	e.printStackTrace();
	        }
		}
		
		public String[] splitMessage(String completeMessage) {
			String[] splittedMessage = completeMessage.split(",");
			return splittedMessage;
		}

		public String byteToString(ByteBuffer byteBuffer)
		{
			byteBuffer.position(0);
			byteBuffer.limit(bytebufferCapacity);
			byte[] bufArr = new byte[byteBuffer.remaining()];
			byteBuffer.get(bufArr);
			return new String(bufArr);
		}
		
		public void run(){
			//Execute the receiveServer here
			executeReceiveServer();
		}
	}
	
	
	public class sendServer extends Thread{
		//Depending upon the type of the message run() will call one of the following methods
		//Class to send messages as requested by Application
		public String typeOfMessage = null;
		public String messageToBeSent =null;
		public String lamportclockvalue = null;
		public String timestampValue = null;
		public String clientList =null;
		
		public sendServer(String messageType, String message, String lamportClock, String currentTimeStamp, String clients){
			this.typeOfMessage = messageType;
			this.messageToBeSent = message;
			this.lamportclockvalue = lamportClock;
			this.timestampValue = currentTimeStamp;
			this.clientList = clients;
		}
		
		public void run(){
			//Insert code to run the specific method
			if( (typeOfMessage.equalsIgnoreCase("isFirst")) || (typeOfMessage.equalsIgnoreCase("isBuffered")) ){
				//Send only if all messages have received ACKs and isFinal
				if((areAllSentMessagesDelivered() && areAllReceivedMessagesisFinal() && isSendReceiveQueueEmpty()) || (typeOfMessage.equalsIgnoreCase("isBuffered"))){
					//Increment the Lamport's clock value since send even is actually happening.
					this.typeOfMessage = "isFirst";
					this.lamportclockvalue = String.valueOf(getLamportsClockNextValue());
					this.sendFirstMessage();
				}
				else{
					//Else buffer it.
					//addToSendReceiveQueue(String sendReceive, String actualmessage, String lamportclockvalue, String clientsenderid, String messagetype, String timestampvalue, String acksent, String clients)
					addToSendReceiveQueue("Send", messageToBeSent, lamportclockvalue, String.valueOf(nodeID), typeOfMessage, timestampValue, null, clientList);
				}
			}
			if(typeOfMessage.equalsIgnoreCase("isFinal")){
				this.sendFinalMessage();
			}
			if(typeOfMessage.equalsIgnoreCase("isACK")){
				//System.out.println("Im going to call sendACKMessage");
				this.sendACKMessage();
			}
			if(typeOfMessage.equalsIgnoreCase("isTerm")){
				this.sendTerminationMessage();
			}
		}
    	
		//Four Send Events
    	//1. isFirst
		//This method will take newMessage and ClientIDs as an input and will send out the message along
		//with the lamport's logical clock value and the message type will be as isFirst.
		public void sendFirstMessage(){
			//clients will be informed with their clientIDs. {1:2:3}
			int loopVariable=0, sentMessagePosition;
			String newMessage = null,aliveStatus="ALIVE";
			String[] tempClientInfo = null;
			String[] tempSentMessageInfo = null;
			String[] splittedClients = clientList.split(":");
			Thread[] newSendClients = new Client[splittedClients.length];
			//long currentTime = System.currentTimeMillis();
	
			//Prepare message to be sent
			newMessage = messageToBeSent +"," + lamportclockvalue +"," + nodeID +"," + typeOfMessage + "," +timestampValue;
			sentMessagePosition = (addNewSentMessage(newMessage, "" )-1);
			//sentMessagePosition = (addNewSentMessage(newMessage, clientList )-1);
			
			//System.out.println("Sending a new multicast message : " +newMessage+ " to clients:" +clientList);
			//Send message to every client!
			while(loopVariable < splittedClients.length){
				//Insert code to send the message to each client
				tempClientInfo = getNodeInfo(splittedClients[loopVariable]);
				newSendClients[loopVariable] = new Client(splittedClients[loopVariable],tempClientInfo[1] , tempClientInfo[2],newMessage);
				newSendClients[loopVariable].start();
				loopVariable++;
			}
			
			//Now wait till the message has been sent to all clients.
			loopVariable=0;
			while(loopVariable < splittedClients.length){
				try{
					newSendClients[loopVariable].join();
				}
				catch (InterruptedException e){
					System.out.println("Unable to join thread.. \nReason: " +e.getCause());					
				}
				loopVariable++;
			}
			
			//Now verify the number of ACK that we should receive
			loopVariable=0;
			while(loopVariable < splittedClients.length){
				tempClientInfo = getNodeInfo(splittedClients[loopVariable]);
				if(tempClientInfo[3].equalsIgnoreCase(aliveStatus)){
					tempSentMessageInfo = getSentMessageInfo(sentMessagePosition);
					
					//Append the clientID to sentMessage to indicate the actual receivers 
					tempSentMessageInfo[1] = tempClientInfo[0]+":"+tempSentMessageInfo[1] ;
					
					//Increment the ACK by 1 for every client that has received the message
					tempSentMessageInfo[2]= String.valueOf((Integer.parseInt(tempSentMessageInfo[2]))+1);
					setSentMessageInfo(sentMessagePosition,tempSentMessageInfo);
				}
				loopVariable++;
			}
		}
    	
    	//2. isFinal
		//After the ACKCount becomes zero for any message ReceiveServer will call this method in order to send the
		//final value of lamport's logical clock value. Message will be decided by ReceiverServer	
    	public void sendFinalMessage(){
			int loopVariable=0;
			String newMessage = null;
			String[] tempClientInfo = null;
			String[] splittedClients = clientList.split(":");
			Thread[] newSendClients = new Client[splittedClients.length];
			
			//Prepare message to be sent to all clients
			newMessage = messageToBeSent + "," + lamportclockvalue +"," + nodeID + "," + typeOfMessage + "," + timestampValue;
			
			//Send a final message to every client that has previously received this message
			loopVariable=0;
			while(loopVariable< splittedClients.length){
				tempClientInfo = getNodeInfo(splittedClients[loopVariable]);
				newSendClients[loopVariable] = new Client(splittedClients[loopVariable],tempClientInfo[1] , tempClientInfo[2],newMessage);
				newSendClients[loopVariable].start();
				loopVariable++;
			}
    	}
		
    	//3. isACK
    	//After receiving a message send an ACK message to the same node.
    	public void sendACKMessage(){
			String newMessage = null;
			String[] tempClientInfo = null;
			Thread newSendClient;
			//System.out.println("Im sending an ACK message & Im in sendACKMessage!!");
			try{
			//Retrieve the senders' information from clientID
				//int tempClientID = Integer.parseInt(clientList);
				//System.out.println("My client ID is: " +tempClientID);
				tempClientInfo = getNodeInfo(clientList);
			}
			catch(Exception e){
				System.out.println("Im getting an exception in sendACKMessage!!");
			}
			
			
			//Prepare an ACK message
			newMessage = messageToBeSent + "," +lamportclockvalue +"," + nodeID + "," +typeOfMessage+ "," +timestampValue;
			
			newSendClient = new Client(clientList, tempClientInfo[1], tempClientInfo[2], newMessage);
			newSendClient.start();
			}
		
    	//4. isTerm
    	//BroadCast this message to all nodes.
		public void sendTerminationMessage(){
			int totalNodes = getNoOfNodes(), loopVariable = 0;
			this.lamportclockvalue = String.valueOf(-1);
			String terminationMessage = null;
			String[] tempClientInfo;
			long terminatioMessageTimestamp = System.currentTimeMillis();
			Thread[] newClients = new Client[totalNodes];
						
			//prepare Message
			terminationMessage = "NoData" + ","+ lamportclockvalue +"," + nodeID + "," + typeOfMessage + "," +terminatioMessageTimestamp ;
			
			loopVariable=0;
			while(loopVariable < totalNodes){
				tempClientInfo = getNodeInfo(String.valueOf(loopVariable));
				newClients[loopVariable] = new Client(String.valueOf(loopVariable), tempClientInfo[1], tempClientInfo[2], terminationMessage);
				newClients[loopVariable].start();
				loopVariable++;
			}
			
			//Now wait till the message has been sent to all clients.
			loopVariable=0;
			while(loopVariable < totalNodes){
				try{
					newClients[loopVariable].join();
				}
				catch (InterruptedException e){
					System.out.println("Unable to join thread.. \nReason: " +e.getCause());					
				}
				loopVariable++;
			}
		}
	}
	
	public class Client extends Thread{
		public int clientID;
		public String clientIPAddress = null;		//Client's IP address
		public String clientPortNumber = null;		//Client's port number
		public String messageToSend = null;			//Data to send
		//public String messageType = null;			//Type of message (either ACK or Message)
		
		public Client (String clientid, String hostAddress, String portNumber, String message){
			clientID =Integer.parseInt(clientid);
			messageToSend = message;
			clientIPAddress = hostAddress;
			clientPortNumber = portNumber;
			//messageType = type;
		}
		
		public void sendMessage() throws Exception{
			SctpChannel sctpChannel;
			SocketAddress socketAddress;
			ByteBuffer byteBuffer = ByteBuffer.allocate(bytebufferCapacity);
			MessageInfo messageInfo;
			try{
				socketAddress = new InetSocketAddress(clientIPAddress,Integer.parseInt(clientPortNumber));
				sctpChannel = SctpChannel.open(socketAddress, 1 ,1 );
				//System.out.println("Sending message to " +clientIPAddress+ " at "+clientPortNumber);
				messageInfo = MessageInfo.createOutgoing(null,0);
				byteBuffer.put(messageToSend.getBytes());
				byteBuffer.flip();
				sctpChannel.send(byteBuffer,messageInfo);
				sctpChannel.close();
			}
			catch (Exception e){
				//Declare Server as dead...
				changeNodeStatus(clientID,clientIPAddress,clientPortNumber,"DEAD" );
				System.out.println("Unable to send message to " +clientIPAddress+ ":" +clientPortNumber);
			}
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			//Run the send message to send a message to individual client
			try{
				sendMessage();
			}
			catch (Exception e){
				System.out.println("Exception in run \nReason: " +e.getCause());
			}
		}
	}
}
