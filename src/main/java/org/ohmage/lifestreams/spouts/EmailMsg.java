package org.ohmage.lifestreams.spouts;

import org.joda.time.DateTime;

import javax.mail.Address;

public class EmailMsg {
    private Address[] senders;
    private Address[] recipients;
    private DateTime sentTime;
    private String subject;
    private String content;
    private boolean attachment;

    public Address[] getSenders() {
        return senders;
    }

    public void setSenders(Address[] senders) {
        this.senders = senders;
    }

    public Address[] getRecipients() {
        return recipients;
    }

    public void setRecipients(Address[] recipients) {
        this.recipients = recipients;
    }

    public DateTime getSentTime() {
        return sentTime;
    }

    public void setSentTime(DateTime sentTime) {
        this.sentTime = sentTime;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public boolean isAttachment() {
        return attachment;
    }

    public void setAttachment(boolean attachment) {
        this.attachment = attachment;
    }

    public EmailMsg(Address[] senders, Address[] recipients, DateTime sentTime, String subject,
                    String content, boolean attachment) {
        super();
        this.senders = senders;
        this.recipients = recipients;
        this.sentTime = sentTime;
        this.subject = subject;
        this.content = content;
        this.attachment = attachment;
    }
}