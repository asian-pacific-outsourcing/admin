package com.apo.admin;
/********************************************************************
* @(#)TouchDB.java 1.00 20101004
* Copyright (c) 2010 by Richard T. Salamone, Jr. All rights reserved.
*
* TouchDB: The database touch table representation of an
* interaction with the contact.
*
* @author Rick Salamone
* @version 1.00 20101004 rts created
* @version 1.01 20101020 rts add methods take uid as short instead of string
* @version 1.02 20101022 rts add fetchMostRecent and fetch contact list methods
* @version 1.03 20101023 rts details field now a Comment rather than a String
* @version 1.04 20101024 rts separated these DB methods out of Touch
* @version 1.05 20101025 rts uses DateTime instead of CallDate
* @version 1.06 20101101 rts uses When instead of DateTime (happy now)
* @version 1.07 20101105 rts added methods to update when and source fields
*******************************************************/
import com.apo.contact.Source;
import com.apo.contact.touch.Touch;
import com.apo.contact.touch.TouchCode;
import com.shanebow.dao.*;
import com.shanebow.dao.DBStatement;
import com.apo.net.Access;
import com.shanebow.util.SBDate;
import com.shanebow.util.SBLog;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

public final class TouchDB
	{
	public static void add( TouchCode code, ContactID contactID, String details )
		{
		add( code, contactID, com.apo.net.Access.getUID(), details );
		}

	public static void add( TouchCode code, ContactID contactID, short uid, String details )
		{
		DBStatement db = null;
		try { add( db = new DBStatement(), code, contactID, SBDate.timeNow(), uid, details ); }
		catch ( Exception e ) { SBLog.write( "Touch failed: " + e ); }
		finally { if ( db != null ) db.close(); }
		}

	public static void add( DBStatement db,
		TouchCode code, ContactID contactID, short uid, String details )
		throws SQLException
		{
		add( db, code, contactID, SBDate.timeNow(), uid, details );
		}

	public static void add( DBStatement db,
		TouchCode aCode, ContactID aRawID, When aWhen, EmpID aEmpID, Comment aDetails )
		throws SQLException
		{
		add(db, aCode, aRawID, aWhen.getLong(), (short)aEmpID.toInt(), aDetails.toString());
		}

	public static void add( DBStatement db,
		TouchCode code, ContactID contactID, long when, short uid, String details )
		throws SQLException
		{
		db.executeUpdate( "INSERT INTO " + Touch.DB_TABLE
		                 + " (contactID,when,employeeID,touchCode,details)"
		                 + " VALUES (" + contactID.dbRepresentation()
		                 +   "," + when + "," + uid
		                 +   "," + code.dbRepresentation()
		                 +   ",'" + details + "');" );
		}

	static Touch read( ResultSet rs )
		throws DataFieldException
		{
		int rsCol = 1;
		ContactID contactID = ContactID.read(rs, rsCol++);
		When  when          = When.read (rs, rsCol++);
		Source  source    = Source.read (rs, rsCol++);
		TouchCode touchCode = TouchCode.read(rs, rsCol++);
		Comment   details   = Comment.read  (rs, rsCol);
		return new Touch( contactID, when, source, touchCode, details );
		}

	public static Touch fetchMostRecent( DBStatement db, ContactID id, TouchCode code )
		{
		String stmt = "SELECT * FROM " + Touch.DB_TABLE
		           + " WHERE contactID = " + id.dbRepresentation()
		           + " AND touchCode = " + code.dbRepresentation() // TouchCode.QUALIFIED
		           + " ORDER BY when desc;";
		ResultSet rs = null;
		try
			{
			rs = db.executeQuery( stmt );
			if (rs.next())
				return read(rs);
			}
		catch (Exception ex) { log( ex.toString() + "\n" + db.getSQL()); }
		finally { db.closeResultSet(rs);}
		return null;
		}

	// public for QualifiedLead
	public static List<ContactID> fetch( DBStatement db, long[] dates, TouchCode code )
		{
		String stmt = "SELECT contactID FROM " + Touch.DB_TABLE
		           + " WHERE when BETWEEN " + dates[0] + " AND " + dates[1]
		           + " AND touchCode = " + code.dbRepresentation()
		           + " ORDER BY when;";
		List<ContactID> ids = new Vector<ContactID>();
		ResultSet rs = null;
		try
			{
			rs = db.executeQuery( stmt );
			while (rs.next())
				ids.add(ContactID.read(rs, 1));
			}
		catch (Exception ex) { log( ex.toString() + "\n" + db.getSQL()); }
		finally { db.closeResultSet(rs);}
		return ids;
		}

	public static boolean update( Touch t, When when )
		{
		DBStatement db = null;
		try { return update( db = new DBStatement(), t, when ); }
		catch ( Exception e ) { SBLog.write( "Update failed: " + e ); }
		finally { if ( db != null ) db.close(); }
		return false;
		}

	public static boolean update( DBStatement db, Touch t, When when )
		throws SQLException
		{
		String stmt = "UPDATE " + Touch.DB_TABLE
		       + " SET when = " + when.dbRepresentation()
		       + " WHERE contactID = " + t.getContactID().dbRepresentation()
		       + " AND when = " + t.getWhen().dbRepresentation()
		       + " AND employeeID = " + t.getSource().dbRepresentation();
		db.executeUpdate( stmt );
		t.setWhen( when );
		return true;
		}

	public static boolean update( Touch t, Source uid )
		{
		DBStatement db = null;
		try { return update( db = new DBStatement(), t, uid ); }
		catch ( Exception e ) { SBLog.write( "Update failed: " + e ); }
		finally { if ( db != null ) db.close(); }
		return false;
		}

	public static boolean update( DBStatement db, Touch t, Source uid )
		throws SQLException
		{
		String stmt = "UPDATE " + Touch.DB_TABLE
		       + " SET employeeID = " + uid.dbRepresentation()
		       + " WHERE contactID = " + t.getContactID().dbRepresentation()
		       + " AND when = " + t.getWhen()
		       + " AND employeeID = " + t.getSource().dbRepresentation();
		db.executeUpdate( stmt );
		t.setSource( uid );
		return true;
		}

	public static List<Touch> fetchHistory( DBStatement db, ContactID id )
		{
		String stmt = "SELECT * FROM " + Touch.DB_TABLE
		           + " WHERE contactID = " + id.dbRepresentation()
		           + " ORDER BY when DESC;";
		List<Touch> history = new Vector<Touch>();
		ResultSet rs = null;
		try
			{
			rs = db.executeQuery( stmt );
			while (rs.next())
				history.add(read(rs));
			}
		catch (Exception ex) { log( ex.toString() + "\n" + db.getSQL()); }
		finally { db.closeResultSet(rs);}
		return history;
		}

	protected static List<Touch> fetchHistory( ContactID id )
		{
		DBStatement db = null;
		try { return fetchHistory( db = new DBStatement(), id ); }
		catch (Exception e) { log( e.toString()); return null; }
		finally { if ( db != null ) db.close(); }
		}

	static void log( String fmt, Object... args )
		{
		SBLog.write( "TouchDB", String.format(fmt, args));
		}
	}
