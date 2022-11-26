package com.apo.admin;
/********************************************************************
* @(#)SysDBDAO.java 1.00 20110208
* Copyright (c) 2011 by Richard T. Salamone, Jr. All rights reserved.
*
* SysDBDAO: An implementation of SysDAO that accesses the system
* information directly from the database.
*
* @author Rick Salamone
* @version 1.00, 20110319 rts initial version
*******************************************************/
import com.shanebow.dao.*;
import com.apo.employee.Role;
import com.apo.net.Message;
import com.apo.net.SysDAO;
import com.shanebow.dao.DBStatement;
import com.shanebow.util.CSV;
import com.shanebow.util.SBDate;
import com.shanebow.util.SBLog;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

public final class SysDBDAO
	extends SysDAO
	{
	public SysDBDAO()
		{
		super();
		DBStatement.connect("", "");
		}

	@Override public void shutdown()
		{
//		DBStatement.disconnect();
		}

	@Override public final long getServerTime()
		{
		return SBDate.timeNow();
		}

	@Override public void purgeWorkQueue( long aAccess )
		throws DataFieldException
		{
		throw new DataFieldException("Not implemented");
		}

	@Override public long sqlCount( String aTables, String aWhereClause )
		throws DataFieldException
		{
		String aSQLStatement = "SELECT COUNT(*) AS x FROM "
			                  + aTables + " " + aWhereClause;
		DBStatement db = null;
		ResultSet rs = null;
		long count = 0;
		try
			{
			db = new DBStatement();
			rs = db.executeQuery(aSQLStatement);
			if ( rs.next())
				count = rs.getLong("x");
			return count;
			}
		catch ( SQLException e )
			{
			String msg = e.getMessage();
			logError( msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) { db.closeResultSet(rs); db.close(); }}
		}

	@Override public int sqlUpdate( String aSQLStatement )
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			db = new DBStatement();
			return db.executeUpdate(aSQLStatement);
			}
		catch ( SQLException e )
			{
			String msg = e.getMessage();
			logError( msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) db.close(); }
		}

	@Override public Message syscmd( byte cmd, String... pieces )
		throws DataFieldException
		{
		throw new DataFieldException("Not implemented");
		}

	@Override public Role getRole( long aAccess )
		throws DataFieldException
		{
		throw new DataFieldException("Not implemented");
		}

	@Override public void setRole( Role aRole )
		throws DataFieldException
		{
		throw new DataFieldException("Not implemented");
		}
	}
