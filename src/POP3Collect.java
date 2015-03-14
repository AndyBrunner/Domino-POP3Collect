
import lotus.notes.addins.JavaServerAddin;
import lotus.notes.internal.MessageQueue;
import java.util.*;
import java.text.DateFormat;
	
public class POP3Collect extends JavaServerAddin {
	// Class variables
	
	static	String		addInName			= "POP3 Collect";
	static	String		copyrightText		= "BETA (c) Copyright ABdata, Andy Brunner";				// TODO
	POP3CollectThread	pop3CollectThread	= null;
		
	MessageQueue		messageQueue		= null;

	String				databaseName		= "POP3Collect.nsf";
	String				versionNumber		= "1.1.0"; 						//TODO: Set current version string. Don't forget to update the first column in view "(Active Connections)"
																			//		and the page "Navigator" in POP3Collect.ntf
	String				messageQueueName	= MSG_Q_PREFIX + addInName.toUpperCase().replaceAll(" ", "");
	String[]			commandlineArgs		= null;
	String				startDateTime		= null;
	
	int 				taskID				= 0;
	
	boolean				stopThread			= false;

	
	// Class constructor without command parameters
	public POP3Collect() {
		this.commandlineArgs = null;	
	}
	
	// Class constructor with command parameters
	public POP3Collect(String[] args) {
		this.commandlineArgs = args;
	}
	
	// Main entry point
	public void runNotes() {
		// Variables
		int messageQueueState = 0;
		
		// Initialize
		setName(addInName);
		
		logMessage("Version " + this.versionNumber + " (Freeware) - " + copyrightText);
		
		this.startDateTime	= DateFormat.getDateTimeInstance().format(new Date());
		
		// Check command line argument
		if (this.commandlineArgs != null) {
			if (this.commandlineArgs.length != 1) {
				logMessage("The command line argument is invalid.");
				logMessage("Use 'Load " + addInName.replaceAll(" ", "") + " -?' for a list of valid arguments.");
				return;
			}
				
			if ((this.commandlineArgs[0].equals("-?")) || (this.commandlineArgs[0].equalsIgnoreCase("HELP"))) {
				logMessage("Purpose:   Retrieve messages from POP3 servers and route them thru SMTP.");
				logMessage("Usage:     Load RunJava " + addInName.replaceAll(" ", "") + " [command]");
				logMessage("");
				logMessage("[command]:");
				logMessage("-?         Display this help screen (or Help).");
				logMessage("database   Name of configuration database (default " + this.databaseName + ").");
				return;
			}
			
			this.databaseName = this.commandlineArgs[0];
		}
		
		/*
		
		// Check if special program version expired
		Calendar endDate = Calendar.getInstance();
		endDate.set(2008, 05 - 1, 30);
				
		if (Calendar.getInstance().after(endDate)) {
			logMessage("This special version has expired");
			cleanup();
			return;
		}
		else
			logMessage("This special version will expire 30/Apr/2008");
		
		*/
		
		// Initialize
		logMessage("Initialization in progress");
		setState("Initialization in progress");
		
		// Create and open the message queue
		this.messageQueue = new MessageQueue();
		
		messageQueueState = this.messageQueue.create(this.messageQueueName, 0, 0);
		
		if (messageQueueState == MessageQueue.ERR_DUPLICATE_MQ) {
			logMessage("The task is already running");
			cleanup();
			return;
		}
		
		if (messageQueueState != NOERROR) {
			logMessage("Error creating the message queue");
			cleanup();
			return;
		}
		
		if (this.messageQueue.open(this.messageQueueName, 0) != NOERROR) {
			logMessage("Error opening the message queue");
			cleanup();
			return;
		}
		
		// Start message processing thread
		
		this.pop3CollectThread = new POP3CollectThread(this, this.databaseName);
		this.pop3CollectThread.start();
			
		// Process commands
		StringBuffer commandLine = new StringBuffer();
		
		while (true) {
			// Set idle state
			setState(null);
			
			// Get next command from the queue (wait 3 seconds)
			messageQueueState = this.messageQueue.get(commandLine, 256, MessageQueue.MQ_WAIT_FOR_MSG, 3000);

			// Quit or Exit
			if (messageQueueState == MessageQueue.ERR_MQ_QUITTING) {
				logMessage("Termination in progress");
				break;				
			}

			// Check if sub task terminated
			if (messageQueueState == MessageQueue.ERR_MQ_TIMEOUT) {
				if (!this.pop3CollectThread.isAlive()) {
					logMessage("Sub task has abnormally terminated");
					this.pop3CollectThread = null;
					break;					
				}
				continue;
			}
			
			if (messageQueueState != NOERROR) {
				logMessage("Error reading from the message queue");
				break;				
			}
			
			if (!processCommand(commandLine))
				break;
		}
	
		// Cleanup
		cleanup();
		
		// Terminate the task
		logMessage("Terminated");
		return;
	}
	
	private boolean processCommand(StringBuffer commandLine) {
		setState("Process user command");
				
		StringTokenizer tokenizer	= new StringTokenizer(commandLine.toString());
		String			token		= null;		
		
		if (tokenizer.countTokens() != 1) {
			logMessage("Use 'Tell " + addInName.replaceAll(" ", "") + " Help' for a list of valid commands.");
			return true;
		}
		
		token = tokenizer.nextToken().trim();
		
		// Command "Shutdown" or "Quit" or "Exit" (Quit and Exit normally handled by system)
		if ((token.equalsIgnoreCase("shutdown")) || (token.equalsIgnoreCase("quit")) || (token.equalsIgnoreCase("exit"))) {
			logMessage("Shutdown command received");
			return false;
		}
		
		// Command "Help" or "?" or "-?"
		if ((token.equalsIgnoreCase("-?")) || (token.equalsIgnoreCase("?")) || (token.equalsIgnoreCase("help"))) {
			logMessage("Purpose:   Retrieve messages from POP3 servers and route them thru SMTP.");
			logMessage("Usage:     Tell " + addInName.replaceAll(" ", "") + " [command]");
			logMessage("");
			logMessage("[command]:");
			logMessage("Help       Display this help screen (or ? or -?).");
			logMessage("Status     Display status information.");
			logMessage("Reload     Reload the configuration from the database (or Refresh).");
			logMessage("Shutdown   Terminate the addin task (or Quit or Exit).");
			return true;
		}
		
		// Command "Reload" or "Refresh"
		if ((token.equalsIgnoreCase("reload")) || (token.equalsIgnoreCase("refresh"))) {
			logMessage("Reloading configuration");
			this.pop3CollectThread.setReload();
			return true;
		}
		
		// Command "Status"
		if (token.equalsIgnoreCase("status")) {			
			logMessage("Started:   " + this.startDateTime);
			logMessage("Database:  " + this.databaseName);
			return true;
		}
		
		// Unsupported command
		logMessage("The command '" + commandLine + "' is not supported.");
		logMessage("Use 'Tell " + addInName.replaceAll(" ", "") + " Help' for a list of valid commands.");
		return true;
				
	}
	
	// Write to console log
	synchronized void logMessage(String message) {
		AddInLogMessageText(addInName + ": " + message, 0);
	}
	
	// Set status line text
	synchronized void setState(String message) {
		// Create the status line necessary
		if (this.taskID == 0)
			this.taskID = AddInCreateStatusLine(addInName);
		
		if (message == null)
			AddInSetStatusLine(this.taskID, "Idle");
		else
			AddInSetStatusLine(this.taskID, message);
	}
	
	// Set termination (called by POP3CollectThread)
	synchronized void setStopThread() {
		this.stopThread = true;
		
		this.messageQueue.putQuitMsg();
	}
			
	// Task cleanup
	private void cleanup() {
		setState("Termination in progress");
		
		try {
			// Close status line
			if (this.taskID != 0) {
				AddInDeleteStatusLine(this.taskID);
				this.taskID = 0;
			}
			
			// Close message queue
			if (this.messageQueue != null) {
				this.messageQueue.close(0);
				this.messageQueue = null;	
			}
			
			// Stop child thread
			if (!this.stopThread) {
				if (this.pop3CollectThread != null)	{
					this.pop3CollectThread.setStopThread();
					
					logMessage("Waiting for all threads to terminate");
					
					while (this.pop3CollectThread.isAlive()) {
						Thread.sleep(250L);
					}
										
					this.pop3CollectThread = null;
				}
			}
			
		} catch (Exception e) {
			logMessage("Error during cleanup of main task: " + e.getMessage());
		}
	}
	
	public void finalize() {
		cleanup();
		super.finalize();
	}
}
