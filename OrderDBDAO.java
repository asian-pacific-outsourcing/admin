package com.apo.admin;
/********************************************************************
* @(#)OrderDBDAO.java 1.00 20110208
* Copyright (c) 2011 by Richard T. Salamone, Jr. All rights reserved.
*
* OrderDBDAO: An interface that defines network IO methods for orders.
*
* @author Rick Salamone
* @version 1.00, 20110208 rts initial version
* @version 1.01, 20110215 rts add randomly generates order id
* @version 1.02, 20110303 rts implemeted add
* @version 1.03, 20110307 rts implemeted update
*******************************************************/
import com.apo.contact.Source;
import com.apo.contact.touch.TouchCode;
import com.apo.order.Order;
import com.apo.order.OrderDAO;
import com.shanebow.dao.*;
import com.shanebow.util.CSV;
import com.shanebow.util.SBDate;
import com.shanebow.ui.SBDialog;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

public final class OrderDBDAO
	extends OrderDAO
	{
	@Override public final When getServerTime()
		{
		return new When(SBDate.timeNow());
		}

	@Override public final OrderID add(Order aOrder)
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			return add(db = new DBStatement(), aOrder ); 
			}
		catch ( SQLException e )
			{
			String msg = "SQL Error: " + e.getMessage();
			log( msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) db.close(); }
		}

//	private static final Random idGenerator = new Random(7);
	private static final Random idGenerator = new Random();
	public final OrderID add( DBStatement db, Order order )
		throws DataFieldException
		{
		When when = getServerTime();

		String stmt = "INSERT INTO " + Order.DB_TABLE;
		for ( int i = 0; i < Order.NUM_DB_FIELDS; i++ )
			stmt += ((i==0)?" (" : ",") + Order.dbField(i);
		stmt += ") VALUES (?";
		for ( int i = 1; i < Order.NUM_DB_FIELDS; i++ )
			if ( i == Order.OPEN || i == Order.ACTIVITY )
				stmt += "," + when.dbRepresentation();
			else
				stmt += "," + order.get(i).dbRepresentation();
		stmt += ")";
		OrderID orderID = null;
		synchronized (OrderDBDAO.class)
			{
			int tries = 0;
			do
				{
				try
					{
					orderID = new OrderID(idGenerator.nextInt(Integer.MAX_VALUE));
					db.executeUpdate( stmt, orderID);
					try
						{
						short uid = (short)((Source)order.get(Order.AO)).id();
						TouchDB.add( db, TouchCode.NEWORDER, order.rawID(),
						              when.getLong(), uid, order.title());
						}
					catch (Exception e)
						{
						logError("New Order Touch failed");
						}
					return orderID;
					}
				catch ( SQLException e )
					{
					// bail if other than duplicate id
					if ( e.getErrorCode() != 0 ) throw new DataFieldException(e);
					}
				}
			while ( ++tries < 10 );
			throw new DataFieldException( "Cannot find valid order ID after " + tries + " tries");
			}
		}

	@Override public final void delete(Order order)
		throws DataFieldException
		{
		throw new DataFieldException("Delete order not supported");
		}

	@Override public final void update( Order aOrder, When aWhen, EmpID aEmpID,
		TouchCode aTouchCode, Comment aComment )
		throws DataFieldException
		{
		DBStatement db = null;
		try
			{
			String sql = "UPDATE " + Order.DB_TABLE;
			for ( int i = 1; i < Order.NUM_DB_FIELDS; i++ )
				sql += ((i==1)? " SET " : ",")
				    + Order.dbField(i) + "=" + aOrder.get(i).dbRepresentation();
			sql += " WHERE id = " + aOrder.id().dbRepresentation();
			ContactID rawID = aOrder.rawID();
			db = new DBStatement();
			db.executeUpdate( sql );
			TouchDB.add( db, aTouchCode, rawID, aWhen, aEmpID, aComment );
			}
		catch ( SQLException e )
			{
			String msg = "SQL Error: " + e.getMessage();
			log( msg + "\nOffending statement: " + db.getSQL());
			throw new DataFieldException(msg);
			}
		finally { if (db != null) db.close(); }
		}

	public final boolean fetch(List<Order> aList, ContactID aID)
		{
		String query = "SELECT * FROM " + Order.DB_TABLE
		             + " WHERE rawID=" + aID.dbRepresentation()
		             + " ORDER by id DESC";
		return fetch( aList, -1, query);
		}

	public final boolean fetch(List<Order> aList, int aMaxRecords, String aQuery)
		{
		return logError("fetch not implemented");
		}

	/*****************
	private void fixUSD(String[] rowData, int... usdFields )
		{
		for ( int field : usdFields )
			rowData[field] = centsToDollars(rowData[field]);
		}

	private String centsToDollars( String cents )
		{
		int length = cents.length();
		switch ( length )
			{
			case 0: return cents;
			case 1: return ".0" + cents;
			case 2: return "." + cents;
			default: return cents.substring( 0, length - 2 )
			             + "." + cents.substring( length - 2 );
			}
		}
	*****************/

	public final boolean supportsDelete()
		{
		return false;
		}
	}
