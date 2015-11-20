package com.pushtorefresh.storio.sqlite.impl;

import android.support.annotation.NonNull;

import com.pushtorefresh.storio.internal.OperatorOnBackpressureBufferAndMerge;
import com.pushtorefresh.storio.sqlite.Changes;

import java.util.Set;

import rx.Observable;
import rx.functions.Func1;

import static com.pushtorefresh.storio.internal.Checks.checkNotNull;

/**
 * FOR INTERNAL USAGE ONLY.
 * <p>
 * Hides RxJava from ClassLoader via separate class.
 */
final class ChangesFilter implements Func1<String, Boolean> {

    @NonNull
    private final Set<String> tables;

    private ChangesFilter(@NonNull Set<String> tables) {
        this.tables = tables;
    }

    @NonNull
    static Observable<Changes> apply(@NonNull Observable<Changes> rxBus, @NonNull Set<String> tables) {
        checkNotNull(tables, "Set of tables can not be null");
        return rxBus
                .flatMap(new Func1<Changes, Observable<String>>() {
                    @Override
                    public Observable<String> call(Changes changes) {
                        return Observable.from(changes.affectedTables());
                    }
                })
                .lift(OperatorOnBackpressureBufferAndMerge.<String>instance())
                .filter(new ChangesFilter(tables))
                .map(new Func1<String, Changes>() {
                    @Override
                    public Changes call(@NonNull String changedTable) {
                        return Changes.newInstance(changedTable);
                    }
                });
    }

    @Override
    @NonNull
    public Boolean call(@NonNull String changedTable) {
        return tables.contains(changedTable);
    }
}
