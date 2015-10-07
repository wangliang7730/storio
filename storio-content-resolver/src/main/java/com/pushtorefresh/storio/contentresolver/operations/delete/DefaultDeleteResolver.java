package com.pushtorefresh.storio.contentresolver.operations.delete;

import android.support.annotation.NonNull;

import com.pushtorefresh.storio.contentresolver.StorIOContentResolver;
import com.pushtorefresh.storio.contentresolver.queries.DeleteQuery;

/**
 * Default implementation of {@link DeleteResolver}.
 * <p>
 * Simply redirects {@link DeleteQuery} to {@link StorIOContentResolver}.
 * <p>
 * Instances of this class are thread-safe.
 */
public abstract class DefaultDeleteResolver<T> extends DeleteResolver<T> {

    /**
     * Converts object of required type to {@link DeleteQuery}.
     *
     * @param storIOContentResolver instance of {@link StorIOContentResolver}.
     * @param object non-null object that should be converted to {@link DeleteQuery}.
     * @return non-null {@link DeleteQuery}.
     */
    @NonNull
    protected abstract DeleteQuery mapToDeleteQuery(@NonNull StorIOContentResolver storIOContentResolver, @NonNull T object);

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public DeleteResult performDelete(@NonNull StorIOContentResolver storIOContentResolver, @NonNull T object) {
        final DeleteQuery deleteQuery = mapToDeleteQuery(storIOContentResolver, object);
        final int numberOfRowsDeleted = storIOContentResolver.internal().delete(deleteQuery);
        return DeleteResult.newInstance(numberOfRowsDeleted, deleteQuery.uri());
    }
}
