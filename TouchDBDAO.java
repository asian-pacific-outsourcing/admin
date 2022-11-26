package com.apo.admin;
/********************************************************************
* @(#)TouchDBDAO.java 1.00 20110218
* Copyright (c) 2011 by Richard T. Salamone, Jr. All rights reserved.
*
* TouchDBDAO: An implementation of RawDAO that accesses the Touch contact
* information over the network via the app serverver.
*
* @author Rick Salamone
* @version 1.00, 20110218 rts initial version
*******************************************************/
import com.apo.contact.touch.Touch;
import com.apo.contact.touch.TouchCode;
import com.apo.contact.touch.TouchDAO;
import com.shanebow.dao.*;
import com.shanebow.util.SBDate;
import com.shanebow.util.SBLog;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

public final class TouchDBDAO
	extends TouchDAO
	{
	@Override public final long getServerTime()
		{
		return SBDate.timeNow();
		}

	@Override public final void add(Touch aTouch)
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			add( db = new DBStatement(), aTouch );
			}
		catch ( Throwable t )
			{
			logError(t.getMessage() + "\nOffending sql: " + db.getSQL());
			throw new DataFieldException(t.toString());
			}
		finally
			{
			if (db != null) db.close();
			}
		}

	private void add(DBStatement db, Touch aTouch )
		throws SQLException
		{
		db.executeUpdate( "INSERT INTO " + Touch.DB_TABLE
		                 + " (contactID,when,employeeID,touchCode,details)"
		                 + " VALUES (" + aTouch.getContactID().dbRepresentation()
		                 +   "," + aTouch.getWhen().dbRepresentation()
									+   "," + aTouch.getSource().dbRepresentation()
		                 +   "," + aTouch.getTouchCode().dbRepresentation()
		                 +   "," + aTouch.getDetails().dbRepresentation()
									+ ");" );
		}

	@Override public List<Touch> fetch(ContactID id)
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			db = new DBStatement();
			return TouchDB.fetchHistory(db, id);
			}
		catch ( SQLException e )
			{
			throw new DataFieldException(e.toString()
			          + "\nOffending statement: " + db.getSQL());
			}
		finally { if (db != null) db.close(); }
		}
	}
