package com.pushtorefresh.storio.contentresolver.operations.get;

import android.database.Cursor;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.annotation.WorkerThread;

import com.pushtorefresh.storio.StorIOException;
import com.pushtorefresh.storio.contentresolver.ContentResolverTypeMapping;
import com.pushtorefresh.storio.contentresolver.StorIOContentResolver;
import com.pushtorefresh.storio.contentresolver.queries.Query;

import rx.Observable;

import static com.pushtorefresh.storio.internal.Checks.checkNotNull;

/**
 * Represents Get Operation for {@link StorIOContentResolver}
 * which performs query that retrieves single object
 * from {@link android.content.ContentProvider}.
 *
 * @param <T> type of result.
 */
public final class PreparedGetObject<T> extends PreparedGet<T> {

    @NonNull
    private final Class<T> type;

    @Nullable
    private final GetResolver<T> explicitGetResolver;

    PreparedGetObject(@NonNull StorIOContentResolver storIOContentResolver,
                      @NonNull Class<T> type,
                      @NonNull Query query,
                      @Nullable GetResolver<T> explicitGetResolver) {
        super(storIOContentResolver, query);
        this.type = type;
        this.explicitGetResolver = explicitGetResolver;
    }

    /**
     * Executes Prepared Operation immediately in current thread.
     * <p>
     * Notice: This is blocking I/O operation that should not be executed on the Main Thread,
     * it can cause ANR (Activity Not Responding dialog), block the UI and drop animations frames.
     * So please, call this method on some background thread. See {@link WorkerThread}.
     *
     *  @return single instance of mapped result. Can be {@code null}, if no items are found.
     */
    @SuppressWarnings({"ConstantConditions", "NullableProblems"})
    @WorkerThread
    @Nullable
    @Override
    public T executeAsBlocking() {
        try {
            final GetResolver<T> getResolver;

            if (explicitGetResolver != null) {
                getResolver = explicitGetResolver;
            } else {
                final ContentResolverTypeMapping<T> typeMapping = storIOContentResolver.internal().typeMapping(type);

                if (typeMapping == null) {
                    throw new IllegalStateException("This type does not have type mapping: " +
                            "type = " + type + "," +
                            "ContentProvider was not touched by this operation, please add type mapping for this type");
                }

                getResolver = typeMapping.getResolver();
            }

            final Cursor cursor = getResolver.performGet(storIOContentResolver, query);

            try {
                final int count = cursor.getCount();

                if (count == 0) {
                    return null;
                }

                cursor.moveToFirst();

                return getResolver.mapFromCursor(cursor);
            } finally {
                cursor.close();
            }
        } catch (Exception exception) {
            throw new StorIOException(exception);
        }
    }

    @NonNull
    @Override
    public Observable<T> createObservable() {
        throw new RuntimeException("not implemented yet");
    }

    /**
     * Builder for {@link PreparedGetObject}.
     *
     * @param <T> type of objects for query.
     */
    public static final class Builder<T> {

        @NonNull
        private final StorIOContentResolver storIOContentResolver;

        @NonNull
        private final Class<T> type;

        public Builder(@NonNull StorIOContentResolver storIOContentResolver, @NonNull Class<T> type) {
            this.storIOContentResolver = storIOContentResolver;
            this.type = type;
        }

        /**
         * Required: Specifies {@link Query} for Get Operation.
         *
         * @param query query.
         * @return builder.
         */
        @NonNull
        public CompleteBuilder<T> withQuery(@NonNull Query query) {
            checkNotNull(query, "Please specify query");
            return new CompleteBuilder<T>(storIOContentResolver, type, query);
        }
    }

    /**
     * Compile-time safe part of builder for {@link PreparedGetObject}.
     *
     * @param <T> type of objects for query.
     */
    public static final class CompleteBuilder<T> {

        @NonNull
        private final StorIOContentResolver storIOContentResolver;

        @NonNull
        private final Class<T> type;

        @NonNull
        private final Query query;

        @Nullable
        private GetResolver<T> getResolver;

        CompleteBuilder(@NonNull StorIOContentResolver storIOContentResolver, @NonNull Class<T> type, @NonNull Query query) {
            this.storIOContentResolver = storIOContentResolver;
            this.type = type;
            this.query = query;
        }

        /**
         * Optional: Specifies {@link GetResolver} for Get Operation
         * which allows you to customize behavior of Get Operation.
         * <p>
         * Can be set via {@link ContentResolverTypeMapping},
         * If value is not set via {@link ContentResolverTypeMapping} — exception will be thrown.
         *
         * @param getResolver GetResolver.
         * @return builder.
         */
        @NonNull
        public CompleteBuilder<T> withGetResolver(@Nullable GetResolver<T> getResolver) {
            this.getResolver = getResolver;
            return this;
        }

        /**
         * Builds new instance of {@link PreparedGetObject}.
         *
         * @return new instance of {@link PreparedGetObject}.
         */
        @NonNull
        public PreparedGetObject<T> prepare() {
            return new PreparedGetObject<T>(
                    storIOContentResolver,
                    type,
                    query,
                    getResolver
            );
        }
    }
}