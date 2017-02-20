package com.activeandroid.query;

/*
 * Copyright (C) 2010 Michael Pardo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import android.database.SQLException;
import android.text.TextUtils;

import com.activeandroid.Cache;
import com.activeandroid.Model;
import com.activeandroid.content.ContentProvider;
import com.activeandroid.query.Join.JoinType;
import com.activeandroid.util.Log;
import com.activeandroid.util.SQLiteUtils;

import java.util.ArrayList;
import java.util.List;

import rx.Observable;

public final class From implements Sqlable {
	private Sqlable mQueryBase;

	private Class<? extends Model> mType;
	private String     mAlias;
	private List<Join> mJoins;
	private final StringBuilder mWhere = new StringBuilder();
	private String  mGroupBy;
	private String  mHaving;
	private String  mOrderBy;
	private String  mLimit;
	private String  mOffset;
	private int     mGroups = 0;
	private boolean mGroupStart = false;

	private List<Object> mArguments;

	public From(Class<? extends Model> table, Sqlable queryBase) {
		mType = table;
		mJoins = new ArrayList<Join>();
		mQueryBase = queryBase;

		mJoins = new ArrayList<Join>();
		mArguments = new ArrayList<Object>();
	}

	// -- Alias

	public From as(String alias) {
		mAlias = alias;
		return this;
	}


	// -- Join

	public Join join(Class<? extends Model> table) {
		Join join = new Join(this, table, null);
		mJoins.add(join);
		return join;
	}

	public Join leftJoin(Class<? extends Model> table) {
		Join join = new Join(this, table, JoinType.LEFT);
		mJoins.add(join);
		return join;
	}

	public Join outerJoin(Class<? extends Model> table) {
		Join join = new Join(this, table, JoinType.OUTER);
		mJoins.add(join);
		return join;
	}

	public Join innerJoin(Class<? extends Model> table) {
		Join join = new Join(this, table, JoinType.INNER);
		mJoins.add(join);
		return join;
	}

	public Join crossJoin(Class<? extends Model> table) {
		Join join = new Join(this, table, JoinType.CROSS);
		mJoins.add(join);
		return join;
	}

	// -- Groups

	public From startGroupAnd() {
		if (mWhere.length() > 0) {
			mWhere.append(" AND");
		}
		mWhere.append(" (");
		mGroups++;
		mGroupStart = true;
		return this;
	}

	public From startGroupOr() {
		if (mWhere.length() > 0) {
			mWhere.append(" OR");
		}
		mWhere.append(" (");
		mGroups++;
		mGroupStart = true;
		return this;
	}

	public From endGroup() {
		if (mGroupStart)
		{
			throw new SQLException("Group is empty !");
		}
		if (mGroups > 0)
		{
			mWhere.append(")");
		}
		mGroups--;
		mGroupStart = false;
		return this;
	}

	// -- Where / and / or

	public From where(String clause) {
		// Chain conditions if a previous condition exists.
		if (mWhere.length() > 0 && ! mGroupStart) {
			mWhere.append(" AND ");
		}
		mWhere.append(clause);
		mGroupStart = false;
		return this;
	}

	public From where(String clause, Object... args) {
		where(clause).addArguments(args);
		return this;
	}

	public From and(String clause) {
		return where(clause);
	}

	public From and(String clause, Object... args) {
		return where(clause, args);
	}

	public From or(String clause) {
		if (mWhere.length() > 0 && ! mGroupStart) {
			mWhere.append(" OR ");
		}
		mWhere.append(clause);
		mGroupStart = false;
		return this;
	}

	public From or(String clause, Object... args) {
		or(clause).addArguments(args);
		return this;
	}

	// -- In / Not In

	public From in(String columnName, Object[] values)
	{
		where(columnName+" IN ").placeholder(values.length).addArguments(values);
		return this;
	}

	public From in(String columnName, List values)
	{
		return in(columnName, values.toArray());
	}

	public From andIn(String columnName, Object[] values)
	{
		return in(columnName, values);
	}

	public From andIn(String columnName, List values)
	{
		return in(columnName, values);
	}

	public From orIn(String columnName, Object[] values)
	{
		or(columnName+" IN ").placeholder(values.length).addArguments(values);
		return this;
	}

	public From orIn(String columnName, List values)
	{
		return orIn(columnName, values.toArray());
	}

	// --

	public From notIn(String columnName, Object[] values)
	{
		where(columnName+" NOT IN ").placeholder(values.length).addArguments(values);
		return this;
	}

	public From notIn(String columnName, List values)
	{
		return in(columnName, values.toArray());
	}

	public From andNotIn(String columnName, Object[] values)
	{
		return in(columnName, values);
	}

	public From andNotIn(String columnName, List values)
	{
		return in(columnName, values);
	}

	public From orNotIn(String columnName, Object[] values)
	{
		or(columnName+" NOT IN ").placeholder(values.length).addArguments(values);
		return this;
	}

	public From orNotIn(String columnName, List values)
	{
		return orIn(columnName, values.toArray());
	}

	// -- Group

	public From groupBy(String groupBy) {
		mGroupBy = groupBy;
		return this;
	}

	// -- Having

	public From having(String having) {
		mHaving = having;
		return this;
	}

	// -- Order

	public From orderBy(String orderBy) {
		mOrderBy = orderBy;
		return this;
	}

	// -- Limit / Offset

	public From limit(int limit) {
		return limit(String.valueOf(limit));
	}

	public From limit(String limit) {
		mLimit = limit;
		return this;
	}

	public From offset(int offset) {
		return offset(String.valueOf(offset));
	}

	public From offset(String offset) {
		mOffset = offset;
		return this;
	}

	// Internal

	void addArguments(Object[] args) {
		for(Object arg : args) {
			if (arg.getClass() == boolean.class || arg.getClass() == Boolean.class) {
				arg = (arg.equals(true) ? 1 : 0);
			}
			mArguments.add(arg);
		}
	}

	private From placeholder(int length)
	{
		mWhere.append("(");
		for(int i=0; i<length; i++) {
			if (i > 0) mWhere.append(",");
			mWhere.append("?");
		}
		mWhere.append(")");
		return this;
	}


	// -- Construct query

	private void addFrom(final StringBuilder sql) {
		sql.append("FROM ");
		sql.append(Cache.getTableName(mType)).append(" ");

		if (mAlias != null) {
			sql.append("AS ");
			sql.append(mAlias);
			sql.append(" ");
		}
	}

	private void addJoins(final StringBuilder sql) {
		for (final Join join : mJoins) {
			sql.append(join.toSql());
		}
	}

	private void addWhere(final StringBuilder sql) {
		if (mWhere.length() > 0) {
			sql.append("WHERE ");
			sql.append(mWhere);
			sql.append(" ");
		}
	}

	private void addGroupBy(final StringBuilder sql) {
		if (mGroupBy != null) {
			sql.append("GROUP BY ");
			sql.append(mGroupBy);
			sql.append(" ");
		}
	}

	private void addHaving(final StringBuilder sql) {
		if (mHaving != null) {
			sql.append("HAVING ");
			sql.append(mHaving);
			sql.append(" ");
		}
	}

	private void addOrderBy(final StringBuilder sql) {
		if (mOrderBy != null) {
			sql.append("ORDER BY ");
			sql.append(mOrderBy);
			sql.append(" ");
		}
	}

	private void addLimit(final StringBuilder sql) {
		if (mLimit != null) {
			sql.append("LIMIT ");
			sql.append(mLimit);
			sql.append(" ");
		}
	}

	private void addOffset(final StringBuilder sql) {
		if (mOffset != null) {
			sql.append("OFFSET ");
			sql.append(mOffset);
			sql.append(" ");
		}
	}

	private String sqlString(final StringBuilder sql) {

		final String sqlString = sql.toString().trim();

		// Don't waste time building the string
		// unless we're going to log it.
		if (Log.isEnabled()) {
			Log.v(sqlString + " " + TextUtils.join(",", getArguments()));
		}

		return sqlString;
	}

	// Get SQL String

	@Override
	public String toSql() {
		final StringBuilder sql = new StringBuilder();
		sql.append(mQueryBase.toSql());

		addFrom(sql);
		addJoins(sql);
		addWhere(sql);
		addGroupBy(sql);
		addHaving(sql);
		addOrderBy(sql);
		addLimit(sql);
		addOffset(sql);

		if (mGroups > 0)
		{
			throw new SQLException("At least one group is not closed in SQL Query : "+sqlString(sql));
		}

		return sqlString(sql);
	}

	public String toExistsSql() {

		final StringBuilder sql = new StringBuilder();
		sql.append("SELECT EXISTS(SELECT 1 ");

		addFrom(sql);
		addJoins(sql);
		addWhere(sql);
		addGroupBy(sql);
		addHaving(sql);
		addLimit(sql);
		addOffset(sql);

		sql.append(")");

		return sqlString(sql);
	}

	public String toCountSql() {

		final StringBuilder sql = new StringBuilder();
		sql.append("SELECT COUNT(*) ");

		addFrom(sql);
		addJoins(sql);
		addWhere(sql);
		addGroupBy(sql);
		addHaving(sql);
		addLimit(sql);
		addOffset(sql);

		return sqlString(sql);
	}

	public <T extends Model> List<T> execute() {
		if (mQueryBase instanceof Select) {
			return SQLiteUtils.rawQuery(mType, toSql(), getArguments());
			
		} else {
			SQLiteUtils.execSql(toSql(), getArguments());
			Cache.getContext().getContentResolver().notifyChange(ContentProvider.createUri(mType, null), null);
			return null;
			
		}
	}

	public <T extends Model> Observable<T> executeRx() {
		if (mQueryBase instanceof Select) {
			return SQLiteUtils.rawRxQuery(mType, toSql(), getArguments());

		} else {
			throw new IllegalArgumentException("Query must be instance of Select");

		}
	}

	public <T extends Model> T executeSingle() {
		if (mQueryBase instanceof Select) {
			limit(1);
			return (T) SQLiteUtils.rawQuerySingle(mType, toSql(), getArguments());
			
		} else {
			limit(1);
			SQLiteUtils.rawQuerySingle(mType, toSql(), getArguments()).delete();
			return null;
			
		}
	}
	
	/**
	 * Gets a value indicating whether the query returns any rows.
	 * @return <code>true</code> if the query returns at least one row; otherwise, <code>false</code>.
	 */
	public boolean exists() {
		return SQLiteUtils.intQuery(toExistsSql(), getArguments()) != 0;
	}

	/**
	 * Gets the number of rows returned by the query.
	 */
	public int count() {
		return SQLiteUtils.intQuery(toCountSql(), getArguments());
	}

	public String[] getArguments() {
		final int size = mArguments.size();
		final String[] args = new String[size];

		for (int i = 0; i < size; i++) {
			args[i] = mArguments.get(i).toString();
		}

		return args;
	}
}
