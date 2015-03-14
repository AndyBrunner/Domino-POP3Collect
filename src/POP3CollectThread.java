import lotus.domino.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.Vector;
import java.util.Date;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.InternetAddress;

import com.sun.mail.pop3.POP3Store;
import com.sun.mail.pop3.POP3Folder;
import com.sun.mail.smtp.SMTPTransport;
import com.sun.mail.smtp.SMTPMessage;

// Class
class POP3CollectThread extends NotesThread {
	
	// Class variables	
	POP3Collect				pop3Collect			= null;
		
	lotus.domino.Session	notesSession		= null;
	Database				notesDatabase		= null;
	
	Vector					connectionEntries	= null;
	Vector					accountEntries		= null;
	Vector					connectionTimes		= null;
	Vector					pop3Checkpoints		= null;
	DateTime				databaseTime		= null;
	String					databaseName		= null;
	String					lastVersionCheck	= null;
	boolean					stopThread			= false;
	boolean					emailSent			= false;
				
	// Thread construction
	POP3CollectThread(POP3Collect pop3Collect, String databaseName) {
		this.pop3Collect	= pop3Collect;
		this.databaseName	= databaseName;
	}
		
	// Main entry point
	public void runNotes() {
		// Access the JVM runtime environment
		Runtime runtime = Runtime.getRuntime();
		
		// Initialize Domino environment
		try {
			this.notesSession	= NotesFactory.createSession();
			
			this.pop3Collect.logMessage("Domino: " + this.notesSession.getNotesVersion().trim() + ", " +
					"Platform: " + this.notesSession.getPlatform() + ", " +
					"JVM Heapsize: " + (runtime.maxMemory() / 1024L) + " KB");
			
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to get Domino environment: " + e.text);
			cleanup();
			return;
		}
		
		// Open the configuration database
		try {
			this.notesDatabase	= this.notesSession.getDatabase(null, this.databaseName);
				
			if (!this.notesDatabase.isOpen()) {
				this.pop3Collect.logMessage("Unable to open database " + this.databaseName);
				cleanup();
				return;
			}
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to open database " + this.databaseName + ": " + e.text);
			cleanup();
			return;
		}
				
		// Check if JavaMaxHeapSpace specified in the Domino notes.ini
		try {
			String javaMaxHeapSize = this.notesSession.getEnvironmentString("JavaMaxHeapSize", true);
			
			if (javaMaxHeapSize.equals(""))	{
				this.pop3Collect.logMessage("The Domino notes.ini parameter JavaMaxHeapSize is not specified. To avoid JVM out-of-memory errors you should specify a value greater than the default setting of 64MB.");
			}
			
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to read the Domino notes.ini environment: " + e.text);
			cleanup();
			return;
		}
				
		// Main loop
		for (long loopCounter = 0L; ;loopCounter++)	{	
			
			// Set task state
			this.pop3Collect.setState(null);
			
			// Check if thread needs to terminate
			if (this.stopThread)
				break;
			
			// Check the memory usage of the JVM once every minute
			if ((loopCounter % 60L) == 0L) {
				long memoryMax			= runtime.maxMemory();
				long memoryUsed			= runtime.totalMemory() - runtime.freeMemory();
				long memoryUsedPercent	= (memoryUsed * 100L) / memoryMax;
				
				if (memoryUsedPercent > 90L) {
					this.pop3Collect.logMessage("Warning: Configured JVM Heapsize " + (memoryMax / 1024L) + " KB, Used " + (memoryUsed / 1024L) + " KB (" + memoryUsedPercent + " %%)");
					this.pop3Collect.logMessage("Consider increasing the Domino notes.ini parameter JavaMaxHeapSize to avoid JVM out-of-memory errors");
					System.gc();
				}
			}
			
			// Wait some time
			waitSeconds(1);
						
			// Check if thread needs to terminate
			if (this.stopThread)
				break;

			// Detect main task termination
			if (!this.pop3Collect.isAlive()) {
				System.out.println("Main task has abnormally terminated");				
				this.pop3Collect = null;
				break;
			}
			
			// Read configuration documents if necessary
			if (!readConfiguration())
				break;
			
			// Check if newer version is available
			checkUpdateServer();

			// Check if no active documents
			if (this.connectionEntries.size() == 0)
				continue;

			if (this.accountEntries.size() == 0)
				continue;
			
			// Loop thru all connection entries
			
			Vector				connectionRow	= null;
			Vector				accountRow		= null;
			javax.mail.Session	pop3Session		= null;
			POP3Store			pop3Store		= null;
			POP3Folder			pop3Folder		= null;
			Message[]			pop3Messages	= null;
			javax.mail.Session	smtpSession		= null;
			SMTPTransport		smtpTransport	= null;
			
			// Loop thru all active connection documents
			for (int connectionIndex = 0; connectionIndex < this.connectionEntries.size(); connectionIndex++) {
				// Check if next connect interval
				Calendar currentTimeStamp	= Calendar.getInstance();
				Calendar lastConnection		= (Calendar) this.connectionTimes.elementAt(connectionIndex); 
				
				if (currentTimeStamp.before(lastConnection))
					continue;
						
				connectionRow = (Vector) this.connectionEntries.elementAt(connectionIndex);

				// Check template version of the configuration database
				String templateVersion = (String) connectionRow.elementAt(0);
				
				if (!templateVersion.equals(this.pop3Collect.versionNumber)) {
					this.pop3Collect.logMessage("The configuration database " + this.databaseName + " is not at the correct version level. Please make sure that the database design is up-to-date.");
					cleanup();
					return;
				}
				
				String		cName				= ((String) connectionRow.elementAt(1)).trim();
				String		cLogLevel			= ((String) connectionRow.elementAt(2)).trim();
				double		cInterval			= ((Double) connectionRow.elementAt(3)).doubleValue();
				String		cPOP3HostName		= ((String) connectionRow.elementAt(4)).trim();
				double		cPOP3Port			= ((Double) connectionRow.elementAt(5)).doubleValue();
				String		cSMTPHostName		= ((String) connectionRow.elementAt(6)).trim();
				double		cSMTPPort			= ((Double) connectionRow.elementAt(7)).doubleValue();								
				String		cSMTPAuth			= ((String) connectionRow.elementAt(8)).trim();
				String		cSMTPUserName		= ((String) connectionRow.elementAt(9)).trim();
				String		cSMTPPassword		= (String) connectionRow.elementAt(10);
				String		cPOP3SSL			= ((String) connectionRow.elementAt(11)).trim();
				String		cSMTPSSL			= ((String) connectionRow.elementAt(12)).trim();
				String		cSchedulingTime		= ((String) connectionRow.elementAt(13)).trim();
								
				// Check time scheduling
				if (cSchedulingTime.equals("Yes")) {
					Calendar	timeStart	= Calendar.getInstance();
					Calendar	timeEnd		= Calendar.getInstance();
					
					try	{
						// Get scheduling start and end time
						
						DateTime cSchedulingStart	= (DateTime) connectionRow.elementAt(14);
						DateTime cSchedulingEnd		= (DateTime) connectionRow.elementAt(15);
						
						timeStart.setTime(cSchedulingStart.toJavaDate());
						timeEnd.setTime(cSchedulingEnd.toJavaDate());
					}
					catch (NotesException e) {
						this.pop3Collect.logMessage("Unable to convert scheduling time: " + e.text);				
					}

					// Check if time schedule is current
					if (currentTimeStamp.before(timeStart))
						continue;
					
					if (currentTimeStamp.after(timeEnd))
						continue;
				}
				
				if (cSMTPAuth.equals("No"))	{
					cSMTPUserName = null;
					cSMTPPassword = null;
				}

				// Set next connection time
				currentTimeStamp.add(Calendar.MINUTE, (int) cInterval);
				this.connectionTimes.setElementAt(currentTimeStamp, connectionIndex);
				
				// Loop thru all active account documents
				
				boolean fPOP3Error = false;
				
				for (int accountIndex = 0; accountIndex < this.accountEntries.size(); accountIndex++) {
					
					fPOP3Error = false;
					
					accountRow = (Vector) this.accountEntries.elementAt(accountIndex);
					
					String aName				= ((String) accountRow.elementAt(0)).trim();
					String aPOP3UserName		= ((String) accountRow.elementAt(1)).trim();
					String aPOP3Password		= (String) accountRow.elementAt(2);
					String aSMTPUserName		= ((String) accountRow.elementAt(3)).trim();
					String aPOP3LeaveMail		= ((String) accountRow.elementAt(4)).trim();
					String aSMTPFallbackAddress	= ((String) accountRow.elementAt(5)).trim();
					
					// Set fallback mail address if not set
					if (!aSMTPUserName.equalsIgnoreCase("*"))
						aSMTPFallbackAddress = aSMTPUserName;
					
					if (cName.equals(aName)) {
						if (cLogLevel.equals("Debug"))
							this.pop3Collect.logMessage("DEBUG: Processing connection " + cName + " - Account " + aPOP3UserName);
						
						// Set task state
						this.pop3Collect.setState("Processing connection " + cName + " - Account " + aPOP3UserName);
						
						// Create POP3 properties
						Properties pop3Props = new Properties();
						
						pop3Props.setProperty("mail.pop3.host", cPOP3HostName);
				        pop3Props.setProperty("mail.pop3.port", Integer.toString((int) cPOP3Port));
				        pop3Props.setProperty("mail.pop3.auth", "true");
				        pop3Props.setProperty("mail.pop3.user", aPOP3UserName);
				        pop3Props.setProperty("mail.pop3.password", aPOP3Password);
				        
						if (cLogLevel.equals("Debug")) {
							pop3Props.setProperty("mail.debug", "true");
							pop3Props.setProperty("mail.pop3.debug", "true");
						}

						// Support SSL/TLS if specified
						if (cPOP3SSL.equals("Yes")) {
					        pop3Props.setProperty("mail.pop3.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
					        pop3Props.setProperty("mail.pop3.socketFactory.fallback", "false");
					        pop3Props.setProperty("mail.pop3.socketFactory.port", Integer.toString((int) cPOP3Port));
						}
				
						// Get a POP3 session
						pop3Session = javax.mail.Session.getInstance(pop3Props);
						
						if (cLogLevel.equals("Debug"))
					    	pop3Session.setDebug(true);
						
						// Get an array with all POP3 messages from inbox
						try {
							// Connect to the POP3 server
							pop3Store = (POP3Store) pop3Session.getStore("pop3");
							pop3Store.connect(cPOP3HostName, aPOP3UserName, aPOP3Password);

							// Open default folder for write access
							pop3Folder = (POP3Folder) pop3Store.getFolder("INBOX");
							pop3Folder.open(Folder.READ_WRITE);
							
							// Get array with all messages
							pop3Messages = pop3Folder.getMessages();
							
						} catch (Exception e) {
							this.pop3Collect.logMessage(cName + " - Unable to read messages from POP3 server " + cPOP3HostName + " - Account " + aPOP3UserName + ": " + e.getMessage());
							fPOP3Error = true;
						}

						if (!fPOP3Error) {
							
							// Send POP3 messages to the SMTP host
							Date	messageDateTime = null;
							Date	newCheckPoint	= null;
							boolean writeCheckpoint = false;
							
							// Loop thru all POP3 messages from this account
							for (int index = 0; index < pop3Messages.length; index++) {

								Vector		pop3Checkpoint			= null;
								String		pUserName				= null;
								Date		savedDateTime			= null;
								int			indexCheckpoint			= 0;
								boolean		skipMessage				= false;
								String		senderAddress			= null;
	
								// Check if valid sender address (to catch some rare cases where the POP3 server does not send a valid "From" address)
								try {
									senderAddress = pop3Messages[index].getFrom()[0].toString();
									new InternetAddress(senderAddress, true).validate();
								} catch (Exception e) {
									senderAddress = "Invalid-From-Address@Unknown.Domain";
								}
								
								if (cLogLevel.equals("Debug"))
									this.pop3Collect.logMessage("DEBUG: Message sender <" + senderAddress + ">");
																
								// Get and check original sent date of POP3 message (to catch some rate cases where the POP3 server does not send valid sent date)
								try {
									// Initialize default value for new POP3 checkpoints
									newCheckPoint = this.notesSession.createDateTime("01/01/1900 01:00:00").toJavaDate();
									messageDateTime = pop3Messages[index].getSentDate();
								} catch (Exception e) {
									messageDateTime = null;
								}
								
								if (messageDateTime == null) {
									this.pop3Collect.logMessage(cName + " - Unable to get sent date of POP3 message. The sent date has been replaced with the current data and time.");
									messageDateTime = new Date();
								}
				
								//TODO: Strange: Sometimes the vector seems to be null
								if (pop3Checkpoints == null) {
									this.pop3Collect.logMessage(cName + " - Checkpoint vector is empty - Creating new vector.");
									pop3Checkpoints = new Vector(0, 0);
								}
								
								// Get corresponding POP3 Checkpoint entry
								for (indexCheckpoint = 0; indexCheckpoint < this.pop3Checkpoints.size(); indexCheckpoint++) {
									pop3Checkpoint	= (Vector) this.pop3Checkpoints.elementAt(indexCheckpoint);
									pUserName		= (String) pop3Checkpoint.elementAt(0);
									
									// Entry found - process it and exit
									if (pUserName.equalsIgnoreCase(aPOP3UserName)) {
										savedDateTime = (Date) pop3Checkpoint.elementAt(1);
										
										if (messageDateTime.compareTo(savedDateTime) > 0) {
											// Newer message found - Update vector
											newCheckPoint = messageDateTime;
											pop3Checkpoint.setElementAt(messageDateTime, 1);
											this.pop3Checkpoints.setElementAt(pop3Checkpoint, indexCheckpoint);
										} else {
											skipMessage = true;
	  									}
	 									break;
									}
								}
	
								// Add a new entry, if not found in vector
								if (savedDateTime == null) {
									Vector newVector = new Vector(0, 0);
									
									newVector.add(aPOP3UserName);
									newVector.add(messageDateTime);
									
									this.pop3Checkpoints.add(newVector);
									
									savedDateTime = messageDateTime;
									newCheckPoint = messageDateTime;
								}
	
								// Check if message should be skipped
								if (aPOP3LeaveMail.equals("Yes")) {
									if (skipMessage) {
										continue;
									}	
								}
								
								// Connect to SMTP server
								if (smtpTransport == null) {
									if (cLogLevel.equals("Debug"))
										this.pop3Collect.logMessage("DEBUG: Connecting to SMTP server " + cSMTPHostName);
									
									Properties smtpProps = new Properties();
									
									smtpProps.setProperty("mail.transport.protocol", "smtp");
									smtpProps.setProperty("mail.smtp.host", cSMTPHostName);
									smtpProps.setProperty("mail.smtp.port", Integer.toString((int) cSMTPPort));
								    
									if (cLogLevel.equals("Debug")) {
										smtpProps.setProperty("mail.debug", "true");
										smtpProps.setProperty("mail.smtp.debug", "true");
									}
								    
									// Set SMTP authentication if required
									if (cSMTPUserName != null && cSMTPPassword != null) {
										smtpProps.setProperty("mail.smtp.auth", "true");
										smtpProps.setProperty("mail.smtp.user", cSMTPUserName);
										smtpProps.setProperty("mail.smtp.password", cSMTPPassword);
									}
								
									// Support SSL/TLS if needed
									if (cSMTPSSL.equals("Yes")) {	
										smtpProps.setProperty("mail.smtp.starttls.enable", "true");
										smtpProps.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
										smtpProps.setProperty("mail.smtp.socketFactory.fallback", "false");
										smtpProps.setProperty("mail.smtp.socketFactory.port", Integer.toString((int) cSMTPPort));								
									}
	
									smtpSession = javax.mail.Session.getInstance(smtpProps);
										
									if (cLogLevel.equals("Debug"))
										smtpSession.setDebug(true);
									
									// Setup transport and connect to SMTP server
									try	{
										smtpTransport = (SMTPTransport) smtpSession.getTransport();
										smtpTransport.connect();
									} catch (Exception e) {
										this.pop3Collect.logMessage(cName + " - Unable to connect to SMTP host " + cSMTPHostName + ": " + e.getMessage());
										break;
									}
								}
									
								// Send message to the SMTP server
								SMTPMessage smtpMessage = null;
								
								try {
									// Create a new message since POP3 messages are read-only
									smtpMessage = new SMTPMessage((MimeMessage) pop3Messages[index]);
									
									// Set the sent date
									smtpMessage.setSentDate(messageDateTime);
									
									// Set the sender address
									smtpMessage.setFrom(new InternetAddress(senderAddress));
																		
									// Set some SMTP header fields 
									smtpMessage.addHeader("X-POP3-Collect-Version", this.pop3Collect.versionNumber);
									smtpMessage.addHeader("X-POP3-Collect-Date", new Date().toString());
									
									// Set the recipient address
									if (aSMTPUserName.equalsIgnoreCase("*")) {
										
										// Find the recipient based on one of the MIME headers or replace it with the fallback address
										String recipientAddress = searchMIMEHeaderForAddress(smtpMessage);
										
										if (recipientAddress == null)
											smtpMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(aSMTPFallbackAddress));
										else
											smtpMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipientAddress));

									} else {
										// Set the address specified in the account document
										smtpMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(aSMTPUserName));
									}
									
									smtpTransport.sendMessage(smtpMessage, smtpMessage.getAllRecipients());
									
									int messageSize = smtpMessage.getSize() / 1024;								
										
									if (messageSize == 0)
										messageSize = 1;
							
									if (aSMTPUserName.equalsIgnoreCase("*"))
										this.pop3Collect.logMessage("Message delivered to " + cSMTPHostName + " from " + aPOP3UserName + " Size: " + messageSize + "K");
									else
										this.pop3Collect.logMessage("Message delivered to " + cSMTPHostName + " for " + aSMTPUserName + " from " + aPOP3UserName + " Size: " + messageSize + "K");
									
									// Mark message to be deleted
									if (aPOP3LeaveMail.equals("No"))
										pop3Messages[index].setFlag(Flags.Flag.DELETED, true);
	
								} catch (Exception e) {
									this.pop3Collect.logMessage(cName + " - Unable to send message to SMTP host " + cSMTPHostName + ": " + e.getMessage());
									
									try {
										String messageText = "The following POP3 message could not be delivered:\n\n" +
															 "Sender:  " + senderAddress + "\n" +
										 					 "Subject: " + smtpMessage.getSubject() + "\n\n" +
										 					 "Error:   " + e.getMessage();
										
										sendMessage(aSMTPFallbackAddress, "POP3 Collect: Non Delivery Notification", messageText);

										
									} catch (Exception ee) {
										this.pop3Collect.logMessage(cName + " - Unable to send notification message to recipient: " + e.getMessage());
									}
									
									break;
								}
								
								writeCheckpoint = true;
							}
							
							// Update POP3 checkpoint
							if (writeCheckpoint) {
								if (!updatePOP3Checkpoint(aPOP3UserName, newCheckPoint))
									break;	
							}
						
						}
						
					    // Close the POP3 connection
						try {
							if (pop3Folder != null)	{
								pop3Folder.close(true);
								pop3Folder = null;
							}
							
							if (pop3Store != null) {
								pop3Store.close();
								pop3Store = null;
							}
						    
						} catch (Exception e) {
							this.pop3Collect.logMessage(cName + " - Unable to close the POP3 server connection: " + e.getMessage());
						}
						
					    // Close the SMTP connection
						try	{
							if (smtpTransport != null) {
								smtpTransport.close();
								smtpTransport = null;
							}
						    
						} catch (Exception e) {
							this.pop3Collect.logMessage(cName + " - Unable to close the SMTP server connection: " + e.getMessage());
						}
					}
					
					// Wait some time after processing one account
					waitSeconds(1);
				}
				
				// Cleanup
				try	{
					if (pop3Folder != null) {
						pop3Folder.close(true);
						pop3Folder = null;
					}
					
					if (pop3Store != null) {
						pop3Store.close();
						pop3Store = null;
					}
					
					if (smtpTransport != null) {
						smtpTransport.close();
					    smtpTransport = null;	
					}
										
				} catch (Exception e) {
					// Ignore errors during cleanup
				}
			}
						
			// Read next					
		}
			
		// Cleanup and terminate
		cleanup();
		return;
	}

	
	// Wait the specified number of seconds
	private void waitSeconds(int seconds) {

		try {
			Thread.sleep((long) seconds * 1000L);
		} catch (Exception e) {
			this.pop3Collect.logMessage("Unable to sleep thread: " + e.getMessage());
		}
	}
	
	// Determine recipient of message
	private String searchMIMEHeaderForAddress(MimeMessage message) {
		
		String recipient = null;

		// Try and validate the MIME headers "X-Envelope-To", "Envelope-To" and "Delivered-To"
		try {
			recipient = validateAddress(message.getHeader("X-Envelope-To", null));
			
			if (recipient != null)
				return recipient;
			
		} catch (MessagingException e) {
		}
		
		try {
			recipient = validateAddress(message.getHeader("Envelope-To", null));
			
			if (recipient != null)
				return recipient;
			
		} catch (MessagingException e) {
		}

		try {
			recipient = validateAddress(message.getHeader("Delivered-To", null));

			if (recipient != null)
				return recipient;
			
		} catch (MessagingException e) {
		}

		return null;		
	}
	
	// Validate Internet address
	private String validateAddress(String address) {
		
		// Check if no address passed
		if (address == null)
			return null;
				
		try {
			InternetAddress emailAddress = new InternetAddress(address, true);
			emailAddress.validate();
			return emailAddress.getAddress();
		} catch (AddressException e) {
			return null;
		}
	}
	
	// Read configuration from database if necessary
	private boolean readConfiguration()	{
		try	{
			// Check if database has been updated
			if (this.databaseTime != null) {
				if (this.databaseTime.timeDifference(this.notesDatabase.getLastModified()) == 0)
					return true;
				else
					this.pop3Collect.logMessage("Reloading configuration due to changes in the configuration database " + this.databaseName);
			}
			
			// Update time stamp
			this.databaseTime = this.notesDatabase.getLastModified();
			
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to read database " + this.databaseName + ": " + e.text);
			return false;
		}
			
		// Get connections and accounts
		this.connectionEntries	= getColumnValues("(ActiveConnections)");
		this.accountEntries		= getColumnValues("(ActiveAccounts)");
		this.pop3Checkpoints	= getColumnValues("(POP3Checkpoints)");

		// Construct array holding the last connection times
		this.connectionTimes = new Vector(0,1);

		// Initialize last connection dates with an overdue date
		if (this.connectionEntries.size() > 0) {
			for (int index = 0; index < this.connectionEntries.size(); index++) {
				Calendar oldDate = Calendar.getInstance();
				oldDate.add(Calendar.YEAR, -1);
				this.connectionTimes.add(oldDate);	
			}
		}
		
		// Convert loaded DateTime objects into Java Date objects
		for (int index = 0; index < this.pop3Checkpoints.size(); index++) {
			Vector pop3Checkpoint = (Vector) this.pop3Checkpoints.elementAt(index);
					
			try	{
				DateTime notesDateTime = (DateTime) pop3Checkpoint.elementAt(1);
				pop3Checkpoint.setElementAt(notesDateTime.toJavaDate(), 1);
			} catch (NotesException e) {
				this.pop3Collect.logMessage("Unable to convert date and time in database " + this.databaseName + ": " + e.text);
				return false;
			}
		}
	
		this.pop3Collect.logMessage("Configuration successfully loaded");
		
		return true;
	}

	// Get all columns values
	private Vector getColumnValues(String viewName) {
		View	notesView	= null;
		Vector	viewData	= new Vector(0, 1);
		
		try {
			// Open view (Sometimes, the view is not updated so we try to make sure it is current)
			notesView = this.notesDatabase.getView(viewName);
			notesView.refresh();
			notesView = this.notesDatabase.getView(viewName);					
			
			if (notesView == null) {
				this.pop3Collect.logMessage("Unable to open view '" + viewName + "' in database " + this.databaseName);
				return viewData;
			}

			// Get all view entries
			ViewEntryCollection notesViewEntryCollection = notesView.getAllEntries();
			
			if (notesViewEntryCollection == null) {
				notesView.recycle();
				return viewData;
			}
			
			// Read thru all view entries
			ViewEntry notesViewEntry = notesViewEntryCollection.getFirstEntry();
			
			while (notesViewEntry != null) {
				viewData.add(notesViewEntry.getColumnValues());
				notesViewEntry = notesViewEntryCollection.getNextEntry();
			}
			
			notesView.recycle();
			
			// Return vector of column values			
			return viewData;
			
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to read view '" + viewName + "' in database " + this.databaseName + ": " + e.text);
			return viewData;
		}
	}
	
	// Update POP3 checkpoint document
	boolean updatePOP3Checkpoint(String pop3UserName, Date dateTime) {
		// Variables
		View		notesView			= null;
		Document	notesDocument		= null;
		boolean 	databaseModified	= false;
		
		// Create or update POP3 checkpoint document
		try {
			// Get view
			notesView = this.notesDatabase.getView("(POP3Checkpoints)");
			
			if (notesView == null) {
				this.pop3Collect.logMessage("Unable to open view '(POP3Checkpoints)' in database " + this.databaseName);
				return false;
			}
			
			// Get document
			notesDocument = notesView.getDocumentByKey(pop3UserName.toLowerCase(), true);
			
			// Check if database has been updated by the user
			if (this.databaseTime.timeDifference(this.notesDatabase.getLastModified()) != 0)
				databaseModified = true;
			
			if (notesDocument == null) {
				// Create new document
				notesDocument = this.notesDatabase.createDocument();
				notesDocument.replaceItemValue("Form", "POP3Checkpoint");
				notesDocument.replaceItemValue("PUserName", pop3UserName);
			}

			// Set new field value and save document
			notesDocument.replaceItemValue("PDateTime", (DateTime) this.notesSession.createDateTime(dateTime));
			notesDocument.computeWithForm(false, false);
			notesDocument.save(true);
			
			// Mark database as current if not already previously modified by the user
			if (!databaseModified)
				this.databaseTime = this.notesDatabase.getLastModified();
			
			// Recycle objects
			notesDocument.recycle();
			notesDocument = null;
			
			notesView.recycle();
			notesView = null;
			
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to update database " + this.databaseName + ": " + e.text);
			return false;
		}
		
		return true;
	}
	
	// Set termination (called by POP3Collect)
	synchronized void setStopThread() {
		this.stopThread = true;
	}
	
	// Force reload of configuration
	synchronized void setReload() {
		this.databaseTime = null;
	}

	// Check if newer POP3 Collect version is available
	synchronized void checkUpdateServer() {
		// Only check the first time during startup and once per day
		String currentDate = DateFormat.getDateInstance(DateFormat.SHORT).format(new Date());
		
		if (currentDate.equals(this.lastVersionCheck))
			return;
		
		this.lastVersionCheck = currentDate;
		
		String serverResponse = null;

		// Check current version against newest available version 
		try {
			// Prepare HTTP connection (send POP3 Collect version, Domino version, Domino platform, Domino server name, IP host name and IP address
			InetAddress			iNetAddress		= InetAddress.getLocalHost();
			String				urlString		= this.pop3Collect.versionNumber + "&" + this.notesSession.getNotesVersion() + "&" + this.notesSession.getPlatform() + "&" + this.notesSession.getServerName() + "&" + iNetAddress.getCanonicalHostName() + "&" + iNetAddress.getHostAddress() + "&";
			URL					updateServer	= new URL("http://ABdata.CH/ABdata/POP3CollectUpdater.nsf/Check?OpenAgent&" + URLEncoder.encode(urlString, "UTF_8"));
			URLConnection		urlConnection	= updateServer.openConnection();
					
			// Connect to update server
			BufferedReader httpInputStream		= new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
			
			// Get the returned data from the server
			serverResponse = httpInputStream.readLine();
						
			// Close HTTP connection
			httpInputStream.close();
			
			if (serverResponse.length() != 5)
				return;
			
		} catch (Exception e) {
			this.pop3Collect.logMessage("Unable to check update server NotesNet.CH for current version");
			return;
		}

		// Return if the current version is running
		if (serverResponse.equals(this.pop3Collect.versionNumber))
			return;

		// Write log message
		this.pop3Collect.logMessage("There is a new POP3 Collect version " + serverResponse + " available on http://ABData.CH/POP3Collect.html");
		
		// Create and send an Email message to the server administrator if not already done once
		if (this.emailSent)
			return;
		
		String emailAdministrator = null;
		
		try {
			// Get email address of administrator
			Database dominoDirectory = this.notesSession.getDatabase(null, "names.nsf");
			
			if (!dominoDirectory.isOpen())
			{
				this.pop3Collect.logMessage("Unable to open the Domino directory.");
				return;
			}
			
			View directoryView = dominoDirectory.getView("($Servers)");
			
			if (directoryView == null) {
				this.pop3Collect.logMessage("Unable to open the view ($Servers) in the Domino directory.");
				return;
			}
			
			Document serverDocument = directoryView.getDocumentByKey(this.notesSession.getServerName());
			
			if (serverDocument == null) {
				this.pop3Collect.logMessage("Unable to access the server document in the Domino directory.");
				return;
			}
			
			emailAdministrator = serverDocument.getItemValueString("Administrator");

			// Free some objects
			serverDocument.recycle();
			directoryView.recycle();
			dominoDirectory.recycle();

			// Check if any name specified
			if (emailAdministrator.length() == 0) {
				this.pop3Collect.logMessage("Unable to send email notification - Administrator field in server document is empty.");
				return;
			}
			
			// Create email message
			Document notesDocument = this.notesDatabase.createDocument();
			
			notesDocument.replaceItemValue("Form", "Memo");
			notesDocument.replaceItemValue("SendTo", emailAdministrator);
			notesDocument.replaceItemValue("Principal", "POP3 Collect");
			notesDocument.replaceItemValue("Subject", "New POP3 Collect version " + serverResponse + " available");
			
			RichTextItem bodyItem = notesDocument.createRichTextItem("Body");
			
			bodyItem.appendText("Dear POP3 Collect Administrator");
			bodyItem.addNewLine(2);
			bodyItem.appendText("There is a new POP3 Collect version " + serverResponse + " available on http://ABData.CH/POP3Collect.html. ");
			bodyItem.addNewLine(2);
			bodyItem.appendText("Thank you for using POP3 Collect.");
			bodyItem.addNewLine();

			notesDocument.send();
		
			// Free the object
			notesDocument.recycle();
			
			// Only send mail notification once
			this.emailSent = true;
			
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to send message to " + emailAdministrator + ": " + e.text);
		}
	}
	
	// Send Email message
	private void sendMessage(String messageRecipient, String messageSubject, String messageBody) {
		try {
			// Create email message
			Document notesDocument = this.notesDatabase.createDocument();
			
			notesDocument.replaceItemValue("Form", "Memo");
			notesDocument.replaceItemValue("SendTo", messageRecipient);
			notesDocument.replaceItemValue("Principal", "POP3 Collect");
			notesDocument.replaceItemValue("Subject", messageSubject);
			
			RichTextItem bodyItem = notesDocument.createRichTextItem("Body");
			
			bodyItem.appendText(messageBody);

			notesDocument.send();
		
			// Free the object
			notesDocument.recycle();
			
		} catch (NotesException e) {
			this.pop3Collect.logMessage("Unable to send notification message to " + messageRecipient + ": " + e.text);
		}
	}
	
	// Cleanup
	private void cleanup() {
		try {				
			// Recycle Notes database
			if (this.notesDatabase != null) {
				this.notesDatabase.recycle();
				this.notesDatabase = null;
			}
				
			// Recycle Notes session
			if (this.notesSession != null) {
				this.notesSession.recycle();
				this.notesSession = null;
			}
			
			// Signal termination to the main thread
			if (!this.stopThread) {
				if (this.pop3Collect != null) {
					this.pop3Collect.setStopThread();
					this.pop3Collect = null;
				}
			}
			
		} catch (NotesException e) {
			System.out.println("Unable to cleanup of sub task: " + e.text);
		}
	}
}