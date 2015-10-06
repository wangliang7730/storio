package com.pushtorefresh.storio.sqlite.operations.put;

import android.content.ContentValues;
import android.database.Cursor;
import android.support.annotation.NonNull;

import com.pushtorefresh.storio.sqlite.StorIOSQLite;
import com.pushtorefresh.storio.sqlite.queries.InsertQuery;
import com.pushtorefresh.storio.sqlite.queries.Query;
import com.pushtorefresh.storio.sqlite.queries.UpdateQuery;

import static com.pushtorefresh.storio.internal.InternalQueries.nullableArrayOfStrings;
import static com.pushtorefresh.storio.internal.InternalQueries.nullableString;

/**
 * Default implementation of {@link PutResolver}.
 * <p>
 * Thread-safe.
 *
 * @param <T> type of objects to put.
 */
public abstract class DefaultPutResolver<T> extends PutResolver<T> {

    /**
     * Converts object of required type to {@link InsertQuery}.
     *
     * @param storIOSQLite {@link StorIOSQLite} instance.
     * @param object       non-null object that should be converted to {@link InsertQuery}.
     * @return non-null {@link InsertQuery}.
     */
    @NonNull
    protected abstract InsertQuery mapToInsertQuery(@NonNull StorIOSQLite storIOSQLite, @NonNull T object);

    /**
     * Converts object of required type to {@link UpdateQuery}.
     *
     * @param storIOSQLite {@link StorIOSQLite} instance.
     * @param object       non-null object that should be converted to {@link UpdateQuery}.
     * @return non-null {@link UpdateQuery}.
     */
    @NonNull
    protected abstract UpdateQuery mapToUpdateQuery(@NonNull StorIOSQLite storIOSQLite, @NonNull T object);

    /**
     * Converts object of required type to {@link ContentValues}.
     *
     * @param storIOSQLite {@link StorIOSQLite} instance.
     * @param object       non-null object that should be converted to {@link ContentValues}.
     * @return non-null {@link ContentValues}.
     */
    @NonNull
    protected abstract ContentValues mapToContentValues(@NonNull StorIOSQLite storIOSQLite, @NonNull T object);

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public PutResult performPut(@NonNull StorIOSQLite storIOSQLite, @NonNull T object) {
        final UpdateQuery updateQuery = mapToUpdateQuery(storIOSQLite, object);

        // for data consistency in concurrent environment, encapsulate Put Operation into transaction
        storIOSQLite.internal().beginTransaction();

        try {
            final Cursor cursor = storIOSQLite.internal().query(Query.builder()
                    .table(updateQuery.table())
                    .where(nullableString(updateQuery.where()))
                    .whereArgs((Object[]) nullableArrayOfStrings(updateQuery.whereArgs()))
                    .build());

            final PutResult putResult;

            try {
                final ContentValues contentValues = mapToContentValues(storIOSQLite, object);

                if (cursor.getCount() == 0) {
                    final InsertQuery insertQuery = mapToInsertQuery(storIOSQLite, object);
                    final long insertedId = storIOSQLite.internal().insert(insertQuery, contentValues);
                    putResult = PutResult.newInsertResult(insertedId, insertQuery.table());
                } else {
                    final int numberOfRowsUpdated = storIOSQLite.internal().update(updateQuery, contentValues);
                    putResult = PutResult.newUpdateResult(numberOfRowsUpdated, updateQuery.table());
                }
            } finally {
                cursor.close();
            }

            // everything okay
            storIOSQLite.internal().setTransactionSuccessful();

            return putResult;
        } finally {
            // in case of bad situations, db won't be affected
            storIOSQLite.internal().endTransaction();
        }
    }
}
