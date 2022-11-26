package com.apo.admin;
/********************************************************************
* @(#)Emailer.java 1.00 20101107
* Copyright (c) 2010 by Richard T. Salamone, Jr. All rights reserved.
*
* Emailer: Sends email to contacts in a background process.
*
* @author Rick Salamone
* @version 1.00 20101107 rts created
*******************************************************/
import com.apo.net.Access;
import com.apo.admin.DlgCfgEmail;
import com.shanebow.dao.EMailAddress;
import com.shanebow.ui.SBDialog;
import com.shanebow.util.SBDate;
import com.shanebow.util.SBLog;
import javax.swing.SwingUtilities;
import java.awt.*;
import java.beans.*;
import java.awt.event.*;
import java.util.LinkedList;
import org.apache.commons.mail.*;

public class Emailer
	implements Runnable
	{
	private static final String MODULE="Emailer";
	static boolean _enabled = true;

	public static void setEnabled(boolean on) {_enabled = on;}
	public static boolean isRunning() { return _enabled; }

	public static void sendTo(EMailAddress recipient)
		{
		if ( !_enabled )
			{
			SBLog.write("Emailer disabled: NOT sent to " + recipient );
			return;
			}
		Emailer task = new Emailer(recipient);
		new Thread(task).start();
		}

	EMailAddress m_recipient;
	private Emailer(EMailAddress recipient)
		{
		m_recipient = recipient;
		}

	private void log ( String fmt, Object... args )
		{
		final String msg = String.format( fmt, args );
		if (SwingUtilities.isEventDispatchThread())
			toLog(msg);
		else SwingUtilities.invokeLater(new Runnable()
			{
			public void run() { toLog(msg); }
			});
		}

	private void toLog(String txt)
		{
		SBLog.write(MODULE, txt );
		}

	/**
	* connect to a remote host
	*/
	public void run()
		{
		try
			{
			log( "SENDING TO: " + m_recipient.toString());
			HtmlEmail email = new HtmlEmail();
			
			DlgCfgEmail.getInstance().compose(email);
			email.addTo(m_recipient.toString());
			if ( Access._serverIP != Access.IP_LOCAL_SERVER )
				email.send();
			else log("DRY RUN!!" );
			}
		catch (EmailException e)
			{
			toLog("EMail Error: " + e.getMessage());
			}
		}
	}
