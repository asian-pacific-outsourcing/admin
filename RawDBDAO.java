package com.apo.admin;
/********************************************************************
* @(#)RawDBDAO.java 1.00 20110208
* Copyright (c) 2011 by Richard T. Salamone, Jr. All rights reserved.
*
* RawDBDAO: An implementation of RawDAO that accesses the Raw contact
* information directly from the database.
*
* @author Rick Salamone
* Modifications as RawLead.java
* 20101003 1.01 rts moved to admin package
* 20101004 1.02 rts add() method here to share between server & admin
* 20101021 1.03 rts read countryID in place of region
* 20101025 1.04 rts read made clunky changes to tqUpdate for Vero
* 20101105 1.05 rts added methods to update a single field
* 20101108 1.06 rts sends email to tq leads
* 20101110 1.07 rts separate message for send email
* 20101125 1.08 rts resets HTR if lead status
* Modifications as RawDBDAO.java
* @version 2.00, 20110208 rts initial extension to RawDAO
* @version 2.01, 20110209 rts moved half of RawLead here, half replaced by Raw
* @version 2.02, 20110220 rts added assign()
* @version 2.03, 20110309 rts added sentMail()
* @version 2.04, 20110523 rts assign handles reassign KOL, TOL, CO
*******************************************************/
import com.apo.contact.Raw;
import com.apo.contact.RawDAO;
import com.apo.contact.Dispo;
import com.apo.contact.Source;
import com.apo.contact.touch.TouchCode;
import com.apo.order.Order;
import com.apo.contact.touch.Touch;
import com.apo.net.Access;
import com.shanebow.dao.DuplicateException;
import com.shanebow.dao.*;
import com.shanebow.dao.DBStatement;
import com.shanebow.util.CSV;
import com.shanebow.util.SBDate;
import com.shanebow.util.SBLog;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

public final class RawDBDAO
	extends RawDAO
	{
	private static final String AO_REASSIGN_DISPOS_LIST
		= "(" + Dispo.TOL.dbRepresentation()
		+ "," + Dispo.KOL.dbRepresentation()
		+ "," + Dispo.CO.dbRepresentation() + ")";

	public RawDBDAO()
		{
		super();
		DBStatement.connect("", "");
		}

	@Override public void shutdown()
		{
		DBStatement.disconnect();
		}

	public static boolean updateField( ContactID id, int field,
		DataField value, short uID )
		{
		DBStatement db = null;
		try { return updateField( db = new DBStatement(), id, field, value, uID ); }
		catch ( Exception e ) { slog( "Update failed: " + e ); }
		finally { if ( db != null ) db.close(); }
		return false;
		}

	public static boolean updateField( DBStatement db,
		ContactID contactID, int field, DataField value, short uID )
		throws DataFieldException
		{
		long when = SBDate.timeNow();
		ResultSet rs = null;
		try
			{
			DataField oldValue = null; // we'll read this from db
			String fieldName = Raw.dbField(field);
			String stmt = "SELECT " + fieldName
			            + " FROM " + Raw.DB_TABLE
		              + " WHERE id = " + contactID.dbRepresentation();
			rs = db.executeQuery(stmt);
			if (rs.next())
				oldValue = Raw.read( field, rs, 1 );
			db.closeResultSet(rs);
			rs = null;

			if ( oldValue.equals(value))
				return false;

			stmt = "UPDATE " + Raw.DB_TABLE
			     + " SET " + fieldName + " = " + value.dbRepresentation()
			     + " WHERE id = " + contactID.dbRepresentation();
			db.executeUpdate( stmt );

			// Make a history entry
			TouchDB.add( db, TouchCode.MODIFIED, contactID, when, uID,
				"" + field + "," + oldValue.csvRepresentation()
				           + "," + value.csvRepresentation());
			}
		catch ( SQLException e )
			{
			throw new DataFieldException(e.toString()
			          + "\nOffending statement: " + db.getSQL());
			}
		finally { if (rs != null) db.closeResultSet(rs); }
		return true;
		}

	static void slog( String fmt, Object... args )
		{
		SBLog.write( "RawDBDAO", String.format(fmt, args));
		}

/********************************************************************
*********************************************************************
********************************************************************/

	@Override public final long getServerTime()
		{
		return SBDate.timeNow();
		}

	@Override public final void keepAlive()
		throws DataFieldException
		{
		// noop
		}

	@Override public void assign( Source aTo, Source aBy, String aCsvRawIDs )
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			db = new DBStatement();
			String sql = "UPDATE " + Raw.DB_TABLE
			           + " SET " + Raw.dbField(Raw.CALLER)
		                     + "=" + aTo.dbRepresentation()
			           + " WHERE id IN (" + aCsvRawIDs + ")";
			db.executeUpdate(sql);
			sql = "UPDATE " + Raw.DB_TABLE
			    + " SET " + Raw.dbField(Raw.DISPO)
		              + "=" + Dispo.ACB.dbRepresentation()
			    + " WHERE " + Raw.dbField(Raw.DISPO)
			            + " IN " + AO_REASSIGN_DISPOS_LIST 
			    + " AND id IN (" + aCsvRawIDs + ")";
			db.executeUpdate(sql);
			Comment details = Comment.parse(aTo.toString());
			touchContacts( db, TouchCode.ASSIGNED, aBy,
				new When(getServerTime()), details, aCsvRawIDs );
			}
		catch ( SQLException e )
			{
			String msg = e.getMessage();
			logError( msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) db.close(); }
		}

	private static final int DAYS = (60 * 60 * 24); // seconds per day
	@Override public void sentMail( Comment aDesc, When aWhenSent, Source aSentBy,
	                                boolean aScheduleCall, String aCsvRawIDs )
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			db = new DBStatement();
			touchContacts(db, TouchCode.MAILSENT, aSentBy,	aWhenSent, aDesc, aCsvRawIDs );
			if ( !aScheduleCall )
				return;
			long eta = aWhenSent.getLong() + 5 * DAYS;
			String sql = "UPDATE " + Raw.DB_TABLE
			           + " SET " + Raw.dbField(Raw.CALLBACK)
		                     + "=" + eta;
			sql += " WHERE id IN (" + aCsvRawIDs + ")";
			db.executeUpdate(sql);
			}
		catch ( SQLException e )
			{
			String msg = e.getMessage();
			logError( msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) db.close(); }
		}

	/**
	* Adds a touch database entry for each contact in a csv list of raw id's
	*/
	private void touchContacts(DBStatement db, TouchCode aTouchCode, Source aWho,
		When aWhen, Comment aDetails, String csvRawIDs )
		throws SQLException
		{
		String[] rawIDs = csvRawIDs.split(",");
		String suffix = "," + aWhen.dbRepresentation()
			            + "," + aWho.id() + "," + aTouchCode.dbRepresentation()
			            + "," + aDetails.dbRepresentation() + ");";
		for ( String rawID : rawIDs )
			db.executeUpdate( "INSERT INTO " + Touch.DB_TABLE
			                + " (contactID,when,employeeID,touchCode,details) VALUES ("
			                + rawID + suffix );
		}

	@Override public void update( Raw aRaw, boolean aReleaseLock,
		TouchCode aTouchCode, String touchDetails, long when, short uID )
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			db = new DBStatement();
			update( db, aRaw, aReleaseLock, aTouchCode, touchDetails, when, uID );
			}
		catch ( SQLException e )
			{
			String msg = "SQL Error: " + e.getMessage();
			log( msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) db.close(); }
		}

	private void update( DBStatement db, Raw aRaw, boolean aReleaseLock,
		TouchCode aTouchCode, String touchDetails, long when, short uID )
		throws DataFieldException
		{
		if ( aReleaseLock )
			aRaw.page().checkIn();

		// ignore dispo is for changes to fields where we aren't changing
		// the contact's dispo - just change as you go touch codes
		boolean ignoreDispo = aTouchCode.equals(TouchCode.EMAILCHG);
		Dispo dispo = aRaw.dispo();
		String htrResetComment = "";
		if ( !ignoreDispo && dispo.resetsHTR())
			{
			htrResetComment = "HTR reset, was " + aRaw.htr();
			aRaw.htr().reset();
			}

		String sql = "UPDATE " + Raw.DB_TABLE;
		for ( int i = 1; i < Raw.NUM_DB_FIELDS; i++ )
			sql += ((i==1)? " SET " : ",")
			    + Raw.dbField(i) + "=" + aRaw.get(i).dbRepresentation();
		sql += " WHERE id = " + aRaw.id().dbRepresentation();

		try
			{
			db.executeUpdate( sql );

			// Make history entries
			String ts = (ignoreDispo? "" : dispo.toString());
			if ( touchDetails != null && !touchDetails.isEmpty()) 
				ts += " " + touchDetails;
			TouchDB.add( db, aTouchCode, aRaw.id(), when, uID, ts );
			if ( ignoreDispo )
				; // no op
			else if ( dispo.equals(Dispo.L))
				{
				TouchDB.add( db, TouchCode.QUALIFIED, aRaw.id(), when+1, uID, htrResetComment);
				sendEmail( db, aRaw.id(), aRaw.eMail(), when + 2, uID );
				}
			else if ( dispo.equals(Dispo.VOL))
				TouchDB.add( db, TouchCode.VEROED, aRaw.id(), when+1, uID, htrResetComment);
			}
		catch ( SQLException e )
			{
			throw new DataFieldException(e.toString()
			          + "\nOffending statement: " + db.getSQL());
			}
		}

	@Override public int countWork(String sqlPhrase)
		throws DataFieldException
		{
		throw new DataFieldException("Operation not supported in stand alone mode");
		}

	@Override public boolean supportsDelete() { return true; }
	@Override public boolean delete( ContactID id )
		throws DataFieldException
		{
		logSeparate( "Delete Contact #" + id );
		DBStatement db = null;
		try
			{
			if ( id == null )
				return false;
			String stmt = "DELETE FROM " + Raw.DB_TABLE
			              + " WHERE id = " + id.dbRepresentation();
			db = new DBStatement();
			db.executeUpdate( stmt );
			stmt = "DELETE FROM " + Touch.DB_TABLE
			              + " WHERE contactID = " + id.dbRepresentation();
			db.executeUpdate( stmt );
			stmt = "DELETE FROM " + Order.DB_TABLE
			              + " WHERE rawID = " + id.dbRepresentation();
			db.executeUpdate( stmt );
			}
		catch (SQLException ex)
			{
			String msg = "Error deleting contact #" + id + ": " + ex.getMessage();
			logError(msg);
			throw new DataFieldException(msg);
			}
		finally { if ( db != null ) db.close(); }
		return logSuccess();
		}

	@Override public Raw getWork(String aCriteria)
		throws DataFieldException
		{
		throw new DataFieldException("Operation not supported in stand alone mode");
		}

	@Override public void release( Raw raw )
		throws DataFieldException
		{
		// No operation
		}

	@Override public Raw fetch(ContactID id)
		throws DataFieldException
		{
		DBStatement db = null;
		try { return fetch( db = new DBStatement(), id ); }
		catch (SQLException ex) { throw new DataFieldException( "Error Retrieving Contact #"
		                                     + id + ": " + ex.getMessage()); }
		finally { if (db != null) db.close(); }
		}

	/**
	* @TODO: Called by QualifiedLead in the RawAdmin program!
	*/
	public static Raw fetch( DBStatement db, ContactID id )
		throws DataFieldException, SQLException
		{
		String stmt = "SELECT * FROM " + Raw.DB_TABLE
		           + " WHERE id = " + id.dbRepresentation();
		ResultSet rs = null;
		try
			{
			rs = db.executeQuery( stmt );
			if (rs.next())
				return(new Raw(rs));
			else throw new DataFieldException( "Contact #" + id + " does not exist" );
			}
//		catch (Exception ex) { slog( ex.toString()); }
		finally { db.closeResultSet(rs);}
		}

	@Override public void fetch(List<Raw> aList, int maxRecords, String query)
		throws DataFieldException
		{
/*********
		String table = Raw.DB_TABLE + " LEFT JOIN " + Touch.DB_TABLE
+ " ON raw.id = touch.contactID ";
		String stmt = "SELECT " + ContactRow.CSV_FIELDS + "\n  FROM " + table
*********/
/*********
		String table = Raw.DB_TABLE;
		String stmt = "SELECT " + ContactRow.CSV_FIELDS + "\n  FROM " + table
		          + "\n WHERE " + whereClause
		          + "\n ORDER BY " + orderClause;
*********/
		logSeparate( "max: " + maxRecords + " Execute SQL: " + query);
		DBStatement db = null;
		ResultSet rs = null;
		int count = 0;
		try
			{
			db = new DBStatement();
			rs = db.executeQuery(query);
			while (rs.next())
				{
				try { aList.add(new Raw(rs)); }
				catch (Exception e) { log( "   Exception add new row: " + e); }
				if ((maxRecords != -1) && (++count >= maxRecords))
					break;
				}
			}
		catch (SQLException ex)
			{
			String msg = ex.toString();
			logError( msg );
			throw new DataFieldException(msg);
			}
		finally { if ( db != null ) { db.closeResultSet(rs); db.close(); }}
		}

	@Override public void reqEmail(ContactID aRawID, EMailAddress aEmail)
		throws DataFieldException
		{
		throw new DataFieldException("Operation not YET implemented for stand alone mode");
		}

	public static void sendEmail(DBStatement db, ContactID contactID,
		EMailAddress address, long when, short uID )
		throws SQLException
		{
		if ( Emailer.isRunning())
			{
			Emailer.sendTo( address );
			TouchDB.add( db, TouchCode.EMAILED, contactID, when, uID,
				DlgCfgEmail.getInstance().getSubject());
			}
		else slog ( "Emailer not running" );
		}

	@Override public void mailReq(Comment aDesc, When aTime, EmpID aEmpID, ContactID aRawID)
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			db = new DBStatement();
			TouchDB.add(db, TouchCode.MAILREQ, aRawID, aTime, aEmpID, aDesc);
			}
		catch (SQLException e)
			{
			String msg = "SQL Error requesting mail: " + e.getMessage();
			logError(	msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) db.close(); }
		}

	@Override public void addRaw( Raw aRaw, EmpID aEmpID, 
		TouchCode aTouchCode, String aSource )
		throws DataFieldException
		{
		try { addLead( aRaw, aEmpID, aTouchCode, aSource ); }
		catch ( DuplicateException e) { throw new DataFieldException(e); }
		}

	@Override public ContactID addLead( Raw aRaw, EmpID aEmpID, 
		TouchCode aTouchCode, String aSource )
		throws DataFieldException, DuplicateException
		{
		DBStatement db = null;
		try
			{
			return addLead( db = new DBStatement(), aRaw, aEmpID, aTouchCode, aSource, getServerTime());
			}
		catch (SQLException e)
			{
			throw new DataFieldException("SQL Error Adding Contact: " + e.getMessage());
			}
		finally { if (db != null) db.close(); }
		}

	private static final String INSERT_PREFIX="INSERT INTO " + Raw.DB_TABLE
				     + " (" + Raw.ID_HEADER + ") VALUES (";

	public ContactID addLead( DBStatement db, Raw aRaw, EmpID aEmpID,
		TouchCode aTouchCode, String host, long when )
		throws DataFieldException, DuplicateException
		{
		ContactID contactID = null; // the return value
		if ( aRaw == null )
			throw new DataFieldException( "Blank contact" );

		DataField[] m_fieldValues = aRaw.getFields();
		ResultSet rs = null;
		String stmt = "";
		try
			{
			// Duplicate check first...
			String phones = makeList(aRaw.phone(), aRaw.mobile(), aRaw.altPhone());
			if ( phones.isEmpty())
				throw new DataFieldException( "At least one phone required" );

			stmt = "SELECT id FROM " + Raw.DB_TABLE
			     + " WHERE name=" + aRaw.name().dbRepresentation()
			     + " AND (phone IN (" + phones + ")"
			     + " OR mobile IN (" + phones + ")"
			     + " OR altPhone IN (" + phones + "))";
			rs = db.executeQuery( stmt );
			if ( rs.next())
				throw new DuplicateException( "of id " + rs.getString(1) + ": " + aRaw );
			rs = db.closeResultSet(rs);

			synchronized (RawDBDAO.class)
				{
				long id = nextContactID(db);
				stmt = INSERT_PREFIX + id;
				for ( int f = 1; f < m_fieldValues.length; f++ )
					stmt += "," + m_fieldValues[f].dbRepresentation();
				stmt += ");";
				db.executeUpdate( stmt );
				contactID = new ContactID(id);
				} // synchronize

			// Make a history entry
			TouchDB.add( db, aTouchCode, contactID, when, (short)aEmpID.toInt(), host );
			}
		catch ( SQLException e )
			{
			throw new DataFieldException(e.toString()
			          + "\nOffending statement: " + db.getSQL());
			}
		finally { db.closeResultSet(rs); }
		return contactID;
		}

	static long lastKnownID = 0;
	private static long nextContactID(DBStatement db)
		throws SQLException
		{
if ( lastKnownID > 0 ) return ++lastKnownID + 1;
		String stmt = "SELECT DISTINCT id FROM " + Raw.DB_TABLE
		            + " WHERE id >= " + lastKnownID
		            + " ORDER BY id DESC;";
		ResultSet rs = db.executeQuery( stmt );
		lastKnownID = rs.next()? rs.getLong(1) : 0;
		db.closeResultSet(rs);
		return 1 + lastKnownID;
		}

	@Override public long nextCheckOutPage()
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			return nextCheckOutPage(db = new DBStatement());
			}
		catch (SQLException e)
			{
			throw new DataFieldException("SQL Error Getting Check out page: " + e.getMessage());
			}
		finally { if (db != null) db.close(); }
		}

	private long nextCheckOutPage(DBStatement db)
		throws SQLException
		{
		String stmt = "SELECT DISTINCT page FROM " + Raw.DB_TABLE
		            + " ORDER BY page DESC;";
		ResultSet rs = db.executeQuery( stmt );
		long page = rs.next()? rs.getLong(1) : Access.MAX_UID;
		db.closeResultSet(rs);
		if ( page < Access.MAX_UID )
			page = Access.MAX_UID;
		return 1 + page;
		}

	@Override public final void checkOut(List<Raw> aList, int aMaxRecords, int aPerPage,
		String aWhereClause, short uid)
		throws DataFieldException
		{
		String sql = "SELECT * FROM " + Raw.DB_TABLE
			                    + " " + aWhereClause
			                    + " ORDER BY disposition DESC, id ASC;";
		logSeparate( "max: " + aMaxRecords + " Execute SQL: " + sql);
		DBStatement db = null;
		DBStatement dbTouch = null;
		ResultSet rs = null;
		long when = getServerTime();
		int count = 0;
		int onPage = 0;
		try
			{
			db = new DBStatement(true); // updatable
			dbTouch = new DBStatement(); // not updatable
			long page = nextCheckOutPage(dbTouch); // use the non-updatable db
			rs = db.executeQuery(sql);
			while (rs.next())
				{
				long id = rs.getLong(1);
				try
					{
					rs.updateLong("page", page );
					rs.updateRow();
					aList.add(new Raw(rs));
					if ( ++onPage >= aPerPage )
						{
						++page;
						onPage = 0;
						}
//					TouchDB.add( dbTouch, TouchCode.CHECKOUT, new ContactID(id), when, uid, "" + page );
					}
				catch (Exception e) { log( "   Exception add new row: " + e); }
				if ((aMaxRecords != -1) && (++count >= aMaxRecords))
					break;
				}
			}
		catch (SQLException ex)
			{
			String msg = ex.toString();
			logError( msg );
			throw new DataFieldException(msg);
			}
		finally
			{
			if ( db != null ) { db.closeResultSet(rs); db.close(); }
			if (  dbTouch != null )  dbTouch.close();
			}
		}
	}
