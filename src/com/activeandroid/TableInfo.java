package com.activeandroid;

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

import android.text.TextUtils;
import android.util.Log;

import com.activeandroid.annotation.Column;
import com.activeandroid.annotation.Table;
import com.activeandroid.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class TableInfo {
	//////////////////////////////////////////////////////////////////////////////////////
	// PRIVATE MEMBERS
	//////////////////////////////////////////////////////////////////////////////////////

	private Class<? extends Model> mType;
	private String mTableName;
	private String mIdName = Table.DEFAULT_ID_NAME;

	private Map<Field, String> mColumnNames = new LinkedHashMap<Field, String>();
    private Map<String, List<Field>> mUniqueGroups = new LinkedHashMap<String, List<Field>>();
    private List<Field> mUniqueKeys = new ArrayList<Field>();

	//////////////////////////////////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////////////////////////////////

	public TableInfo(Class<? extends Model> type) {
		mType = type;

		final Table tableAnnotation = type.getAnnotation(Table.class);

        if (tableAnnotation != null) {
			mTableName = tableAnnotation.name();
			mIdName = tableAnnotation.id();
		}
		else {
			mTableName = type.getSimpleName();
        }

        // Manually add the id column since it is not declared like the other columns.
        Field idField = getIdField(type);
        mColumnNames.put(idField, mIdName);

        List<Field> fields = new LinkedList<Field>(ReflectionUtils.getDeclaredColumnFields(type));
        Collections.reverse(fields);

        for (Field field : fields) {

            // Fields

            if (field.isAnnotationPresent(Column.class)) {
                final Column columnAnnotation = field.getAnnotation(Column.class);
                String columnName = columnAnnotation.name();
                if (TextUtils.isEmpty(columnName)) {
                    columnName = field.getName();
                }

                mColumnNames.put(field, columnName);
            }

            // "Unique on update" fields

            Column column = field.getAnnotation(Column.class);
            Object value = null;
            if (column == null) {
                continue; // Not a column
            }
            if (! column.unique() && column.uniqueGroups().length == 0) {
                continue; // Not unique key
            }
            Column.ConflictAction   conflictAction = column.onUniqueConflict();
            Column.ConflictAction[] conflictActions = column.onUniqueConflicts();
            if (! Column.ConflictAction.UPDATE.equals(conflictAction) && conflictActions.length == 0) {
                continue; // No "UPDATE" action
            }
            if (Column.ConflictAction.UPDATE.equals(conflictAction)) {
                mUniqueKeys.add(field);
            }
            String[] uniqueGroups = column.uniqueGroups();
            Column.ConflictAction[] uniqueConflicts = column.onUniqueConflicts();
            for(int i=0; i<uniqueGroups.length; i++) {
                if (uniqueConflicts.length <= i) {
                    continue;
                }
                if (Column.ConflictAction.UPDATE.equals(uniqueConflicts[i])) {
                    String group = uniqueGroups[i];
                    if (mUniqueGroups.get(group) == null) {
                        mUniqueGroups.put(group, new LinkedList<Field>());
                    }
                    mUniqueGroups.get(group).add(field);
                }
            }
        } // for each field

    }

	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	public Class<? extends Model> getType() {
		return mType;
	}

	public String getTableName() {
		return mTableName;
	}

	public String getIdName() {
		return mIdName;
	}

	public Collection<Field> getFields() {
		return mColumnNames.keySet();
	}

	public String getColumnName(Field field) {
		return mColumnNames.get(field);
	}

    public boolean hasOnUpdateFields() {
        return ! mUniqueGroups.isEmpty() || ! mUniqueKeys.isEmpty();
    }

    public Collection<List<Field>> getUniqueGroups() {
        return mUniqueGroups.values();
    }

    public Collection<Field> getUniqueFields() {
        return mUniqueKeys;
    }

    private Field getIdField(Class<?> type) {
        if (type.equals(Model.class)) {
            try {
                return type.getDeclaredField("mId");
            }
            catch (NoSuchFieldException e) {
                Log.e("Impossible!", e.toString());
            }
        }
        else if (type.getSuperclass() != null) {
            return getIdField(type.getSuperclass());
        }

        return null;
    }

}
