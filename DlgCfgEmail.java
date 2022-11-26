package com.apo.admin;
/********************************************************************
* @(#)DlgCfgEmail.java	1.00 05/10/10
* Copyright 2010 by Richard T. Salamone, Jr. All rights reserved.
*
* DlgCfgEmail: Checks out telephone qualified leads for vero.
*
* @author Rick Salamone
* @version 1.00 20101027 rts created
* @version 1.00 20101122 rts reads file correcting for charset issues
*******************************************************/
import org.apache.commons.mail.*;
//import com.apo.util.PropertyLoader;
import com.shanebow.ui.SBDialog;
import com.shanebow.util.SBDate;
import com.shanebow.util.SBLog;
import com.shanebow.ui.layout.LabeledPairPanel;
import com.shanebow.ui.SBDialog;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.nio.charset.Charset ;
import java.util.Properties;
import javax.swing.JTextArea;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.JTextField;

public final class DlgCfgEmail
	extends JDialog
	implements ActionListener
	{
	private static final DlgCfgEmail _me = new DlgCfgEmail();
	public static final DlgCfgEmail getInstance() { return _me; }
	public static void launch()
		{
		_me.setVisible(true);
		}

	private static final String CMD_CLOSE="OK";

	private DlgCfgEmail()
		{
		super((JFrame)null, "Email Test", false);
		JPanel top = new JPanel(new BorderLayout());
		top.setBorder(new EmptyBorder(5, 10, 2, 10));
		top.add( emailSettingsPanel(),  BorderLayout.NORTH);
		top.add( mainPanel(), BorderLayout.CENTER);
		top.add( btnPanel(),  BorderLayout.SOUTH);
		top.setPreferredSize(new Dimension(600,475)); // w, h
		setContentPane(top);
		pack();
		}
/*********
news@leebyers.com
f1ghtm3p13a53
mail.leebyers.com
*********/
	private String EMAIL_DOMAIN = "leebyers.com";
	JTextField tfHostName = new JTextField("mail." + EMAIL_DOMAIN);
	JTextField tfPort = new JTextField("25");
	JTextField tfUserName = new JTextField("news@" + EMAIL_DOMAIN);
	JTextField tfPassword = new JPasswordField("f1ghtm3p13a53");
	JTextField tfFrom = new JTextField("news@" + EMAIL_DOMAIN);
	JTextField tfBounceAddress = new JTextField("news@" + EMAIL_DOMAIN);
	JTextField tfSubject = new JTextField("Welcome to Lee Byers");

	TextFileArea taHTML = new TextFileArea("email_welcome.html");
	TextFileArea taTXT = new TextFileArea("email_welcome.txt");

	private JComponent emailSettingsPanel()
		{
		LabeledPairPanel p = new LabeledPairPanel();
		p.addRow( "SMTP Host: ", tfHostName );
		p.addRow( "SMPT Port: ", tfPort );
		p.addRow( "SMTP User: ", tfUserName );
		p.addRow( "Password: ",  tfPassword );
		p.addRow( "Bounce To: ", tfBounceAddress );
		p.addRow( "From: ", tfFrom );
		p.addRow( "Subject: ", tfSubject );
		return p;
		}

	private JComponent mainPanel()
		{
		JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);

//		taHTML.setPreferredSize( new Dimension(50,500));
		JScrollPane scroller = new JScrollPane(taHTML);
		scroller.setBorder(BorderFactory.createTitledBorder("HTML Content: " + taHTML));
		splitPane.setTopComponent(scroller);

		taTXT.setForeground(Color.BLACK);
		scroller = new JScrollPane(taTXT);
		scroller.setBorder(BorderFactory.createTitledBorder("TEXT Content: " + taTXT));
		splitPane.setBottomComponent(scroller);
		splitPane.setDividerLocation(185); //XXX: ignored in some releases
		return splitPane;
		}

	private JPanel btnPanel()
		{
		JPanel p = new JPanel();
		p.add( makeButton(CMD_CLOSE));
		return p;
		}

	private JButton makeButton(String caption)
		{
		JButton b = new JButton(caption);
		b.addActionListener(this);
		return b;
		}

	public void actionPerformed(ActionEvent e)
		{
		String cmd = e.getActionCommand();
		if ( cmd.equals(CMD_CLOSE))
			setVisible(false);
		}

	public String getSubject() { return tfSubject.getText(); }

	public final void compose( HtmlEmail email )
		throws EmailException
		{
		email.setHostName(tfHostName.getText());
		if ( !tfUserName.getText().isEmpty())
			email.setAuthentication(tfUserName.getText(), tfPassword.getText());
		email.setSmtpPort(Integer.parseInt(tfPort.getText()));
		email.setFrom(tfFrom.getText());
		email.setSubject(tfSubject.getText());
		email.setTextMsg(taTXT.getText());
		email.setHtmlMsg(taHTML.getText());
		if ( !tfBounceAddress.getText().isEmpty())
			email.setBounceAddress(tfBounceAddress.getText());
		email.setDebug(true);
		}
	}

class TextFileArea extends JTextArea
	{
	public  static final Font FONT = new Font(Font.MONOSPACED, Font.BOLD, 12);
	private static final String NEWLINE = "\n";

	String filespec;
	JTextArea m_textArea;

	public TextFileArea()
		{
		this( "" );
		}

	public TextFileArea(String filespec)
		{
		super();
		setText("");
		this.filespec = filespec;
		setMargin(new Insets(5, 5, 5, 5));
		setFont(FONT);
		setCaretPosition(0);
		setForeground(Color.BLUE);
		setTabSize(3);
		appendFile(filespec);
		}

	public void appendLine( String line )
		{
		append(line + NEWLINE);
		setCaretPosition(getDocument().getLength());
		}

/**************
	public void appendFile(String filespec)
		{
		try
			{
			BufferedReader reader = new BufferedReader(
										new FileReader(filespec));
			String in;
			while ((in = reader.readLine()) != null)
				append(in + NEWLINE);
			reader.close();
			setCaretPosition(0);
			}
		catch (Exception e)
			{
			e.printStackTrace();
			return;
			}
		}
**************/
	public void appendFile(String filespec)
		{
		try
			{
			InputStreamReader reader = new InputStreamReader(
										new FileInputStream(filespec),
Charset.forName("US-ASCII")); // "ISO-8859-1")); //   // UTF-8,  UTF-16
			int in;
			int count = 0;
			while ((in = reader.read()) != -1)
				if ( in < 128 )
					{
					if ( count++ > 0 || in > 10 )
						append("" + (char)in);
					}
			reader.close();
			setCaretPosition(0);
			}
		catch (Exception e)
			{
			e.printStackTrace();
			return;
			}
		}

	protected void clearLog()
		{
		setText("");
		}

	public String toString() { return filespec; }
	}
