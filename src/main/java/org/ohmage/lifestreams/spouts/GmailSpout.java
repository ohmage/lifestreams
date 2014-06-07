package org.ohmage.lifestreams.spouts;

import java.io.IOException;
import java.security.Security;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.SearchTerm;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.AccessTokenData;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.code.samples.oauth2.OAuth2Authenticator;
import com.google.code.samples.oauth2.OAuth2Authenticator.OAuth2Provider;
import com.sun.mail.imap.IMAPStore;

public class GmailSpout extends OAuthProtectedDataSpout<EmailMsg> {
	private static final boolean DEBUG = false;
	private static final int GMAIL_IMAP_PORT = 993;
	private static final String GMAIL_IMAP_URL = "imap.gmail.com";
	private static final String GMAIL_SENT_MAIL_INBOX = "[Gmail]/Sent Mail";
	public static final Pattern providerNamePattern = Pattern.compile("gmail");
	public static final Pattern scopePattern = Pattern.compile("https\\://mail\\.google\\.com/");
	final IProvider provider;
	final ObjectMapper mapper = new ObjectMapper();

	public GmailSpout(IProvider resourceProvider, OhmageStream accessTokenStream, DateTime since) {
		super(accessTokenStream, providerNamePattern, scopePattern , since, 1, TimeUnit.HOURS);
		this.provider =  resourceProvider;
	}
	private String getEmailFromMetaInfo(Object metaInfo){
		ObjectNode node = mapper.convertValue(metaInfo, ObjectNode.class);
		JsonNode emailNode = node.findValue("email");
		if(emailNode != null && emailNode.isTextual()){
			return emailNode.asText();
		}
		return null;
	}
	private String getContentFor(Message msg) throws IOException, MessagingException{
		String content = "";
		if(msg.getContent() instanceof String){
    		content = (String) msg.getContent();
    	}else if(msg.getContent() instanceof Multipart){
    		
    		Multipart multipart = (Multipart) msg.getContent();
    	    for (int j = 0; j < multipart.getCount(); j++) {
    	        BodyPart bodyPart = multipart.getBodyPart(j);
    	        String disposition = bodyPart.getDisposition();
    	          if (disposition != null && (disposition.equalsIgnoreCase("ATTACHMENT"))) { // BodyPart.ATTACHMENT doesn't work for gmail
    	              logger.trace("Mail have some attachment");
    	                                       
    	            }
    	          else { 
    	              //System.out.println("Body: "+bodyPart.getContent());
    	              content += bodyPart.getContent().toString();
    	            }
    	    }
    	}
		return content;
	}
	private boolean checkAttachment(Message msg) throws IOException, MessagingException{
		if(msg.getContent() instanceof Multipart){
    		Multipart multipart = (Multipart) msg.getContent();
    	    for (int j = 0; j < multipart.getCount(); j++) {
		        BodyPart bodyPart = multipart.getBodyPart(j);
		        String disposition = bodyPart.getDisposition();
		        if (disposition != null && (disposition.equalsIgnoreCase("ATTACHMENT"))) { // BodyPart.ATTACHMENT doesn't work for gmail
		              return true;                           
		        }
    	    }
    	}
		return false;
	}
	@Override
	protected Iterator<StreamRecord<EmailMsg>> getIteratorFor(OhmageUser user,
			DateTime since) {
		try{
			AccessTokenData tokenData = this.getLatestAccessTokenRecord(user);
			if(tokenData == null){
				return null;
			}
	
			String accessToken = tokenData.getToken().getAccessToken();
			String email = getEmailFromMetaInfo(tokenData.getMetaInfo());
			if(email == null){
				throw new Exception("Cannot find any email address in the token data"
									+  mapper.writeValueAsString(tokenData));
			}
			IMAPStore store = null;
			try{
				// try connect to imap
				store = OAuth2Authenticator.connectToImap(GMAIL_IMAP_URL, GMAIL_IMAP_PORT, "changun.tw",  accessToken, DEBUG);
				throw new Exception();
			}catch(Exception e){
				// if failed, try to refresh the token and try to connect again
				accessToken = refreshAndUploadToken(tokenData.getToken(), user, provider).getAccessToken();
				store = OAuth2Authenticator.connectToImap(GMAIL_IMAP_URL, GMAIL_IMAP_PORT, "changun.tw",  accessToken, DEBUG);
			}
	        final IMAPStore imapStore = store;
	        Folder inbox = imapStore.getFolder(GMAIL_SENT_MAIL_INBOX);
	        inbox.open(Folder.READ_ONLY);
	        // search
	        SearchTerm newerThan = new ReceivedDateTerm(ComparisonTerm.GE, since.toDate());
	        Message messages[] = inbox.search(newerThan);
	        final Iterator<Message> msgIter = Arrays.asList(messages).iterator();
	        
	        return new ICloseableIterator<StreamRecord<EmailMsg>>(){

				@Override
				public boolean hasNext() {
					return msgIter.hasNext();
				}

				@Override
				public StreamRecord<EmailMsg> next() {
					try{
						Message msg = msgIter.next();
						boolean attachment = checkAttachment(msg);	
			        	Address[] senders = msg.getFrom();
			        	Address[] recipients = msg.getAllRecipients();
			        	String subject = msg.getSubject();
			        	String content = getContentFor(msg);
			        	DateTime sentTime = new DateTime(msg.getSentDate());
			        	// create mail stream record
			        	EmailMsg mail = new EmailMsg(senders, recipients, sentTime, subject, content, attachment);
			        	StreamRecord<EmailMsg> rec = new StreamRecord<EmailMsg>();
			        	rec.setData(mail);
			        	rec.setTimestamp(sentTime);
			        	return rec;
					}catch(Exception e){
						throw new RuntimeException(e);
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}

				@Override
				public void close() throws IOException {
					try {
						imapStore.close();
					} catch (MessagingException e) {
						logger.error("Fail to close imap store", e);
					}
				}
	        	
	        };
		}catch(Exception e){
			logger.error("Error retreiving sent mails for " + user.getUsername(), e);
		}
		return null;

	}
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		// register google xoauth provider
		Security.addProvider(new OAuth2Provider());
	}

}
