package com.pushtorefresh.storio.sqlite.integration;

import android.support.annotation.NonNull;

import com.pushtorefresh.storio.sqlite.BuildConfig;
import com.pushtorefresh.storio.sqlite.Changes;
import com.pushtorefresh.storio.sqlite.queries.Query;
import com.pushtorefresh.storio.test.AbstractEmissionChecker;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricGradleTestRunner;
import org.robolectric.annotation.Config;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.observers.TestSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(RobolectricGradleTestRunner.class)
@Config(constants = BuildConfig.class, sdk = 21)
public class RxQueryTest extends BaseTest {

    private class EmissionChecker extends AbstractEmissionChecker<List<User>> {

        public EmissionChecker(@NonNull Queue<List<User>> expected) {
            super(expected);
        }

        @Override
        @NonNull
        public Subscription subscribe() {
            return storIOSQLite
                    .get()
                    .listOfObjects(User.class)
                    .withQuery(UserTableMeta.QUERY_ALL)
                    .prepare()
                    .createObservable()
                    .subscribe(new Action1<List<User>>() {
                        @Override
                        public void call(List<User> users) {
                            onNextObtained(users);
                        }
                    });
        }
    }

    @Test
    public void insertEmission() {
        final List<User> initialUsers = putUsersBlocking(10);
        final List<User> usersForInsert = TestFactory.newUsers(10);
        final List<User> allUsers = new ArrayList<User>(initialUsers.size() + usersForInsert.size());

        allUsers.addAll(initialUsers);
        allUsers.addAll(usersForInsert);

        final Queue<List<User>> expectedUsers = new LinkedList<List<User>>();
        expectedUsers.add(initialUsers);
        expectedUsers.add(allUsers);

        final EmissionChecker emissionChecker = new EmissionChecker(expectedUsers);
        final Subscription subscription = emissionChecker.subscribe();

        // Should receive initial users
        emissionChecker.awaitNextExpectedValue();

        putUsersBlocking(usersForInsert);

        // Should receive initial users + inserted users
        emissionChecker.awaitNextExpectedValue();

        emissionChecker.assertThatNoExpectedValuesLeft();

        subscription.unsubscribe();
    }

    @Test
    public void updateEmission() {
        final List<User> users = putUsersBlocking(10);

        final Queue<List<User>> expectedUsers = new LinkedList<List<User>>();

        final List<User> updatedList = new ArrayList<User>(users.size());

        int count = 1;
        for (User user : users) {
            updatedList.add(User.newInstance(user.id(), "new_email" + count++));
        }
        expectedUsers.add(users);
        expectedUsers.add(updatedList);
        final EmissionChecker emissionChecker = new EmissionChecker(expectedUsers);
        final Subscription subscription = emissionChecker.subscribe();

        // Should receive all users
        emissionChecker.awaitNextExpectedValue();

        storIOSQLite
                .put()
                .objects(updatedList)
                .prepare()
                .executeAsBlocking();

        // Should receive updated users
        emissionChecker.awaitNextExpectedValue();

        emissionChecker.assertThatNoExpectedValuesLeft();

        subscription.unsubscribe();
    }

    @Test
    public void deleteEmission() {
        final List<User> usersThatShouldBeSaved = TestFactory.newUsers(10);
        final List<User> usersThatShouldBeDeleted = TestFactory.newUsers(10);
        final List<User> allUsers = new ArrayList<User>();

        allUsers.addAll(usersThatShouldBeSaved);
        allUsers.addAll(usersThatShouldBeDeleted);

        putUsersBlocking(allUsers);

        final Queue<List<User>> expectedUsers = new LinkedList<List<User>>();

        expectedUsers.add(allUsers);
        expectedUsers.add(usersThatShouldBeSaved);

        final EmissionChecker emissionChecker = new EmissionChecker(expectedUsers);
        final Subscription subscription = emissionChecker.subscribe();

        // Should receive all users
        emissionChecker.awaitNextExpectedValue();

        deleteUsersBlocking(usersThatShouldBeDeleted);

        // Should receive users that should be saved
        emissionChecker.awaitNextExpectedValue();

        emissionChecker.assertThatNoExpectedValuesLeft();

        subscription.unsubscribe();
    }

    @Test
    public void parallelWritesWithoutTransaction() {
        final int numberOfParallelWorkers = 50;

        TestSubscriber<Changes> testSubscriber = new TestSubscriber<Changes>();

        storIOSQLite
                .observeChangesInTable(TweetTableMeta.TABLE)
                .take(numberOfParallelWorkers)
                .subscribe(testSubscriber);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        for (int i = 0; i < numberOfParallelWorkers; i++) {
            final int copyOfCurrentI = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    storIOSQLite
                            .put()
                            .object(Tweet.newInstance(null, 1L, "Some text: " + copyOfCurrentI))
                            .prepare()
                            .executeAsBlocking();
                }
            }).start();
        }

        // Release the KRAKEN!
        countDownLatch.countDown();

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        assertThat(testSubscriber.getOnNextEvents()).hasSize(numberOfParallelWorkers);
    }

    @Test
    public void nestedTransaction() {
        storIOSQLite.internal().beginTransaction();

        storIOSQLite.internal().beginTransaction();

        storIOSQLite.internal().setTransactionSuccessful();
        storIOSQLite.internal().endTransaction();

        storIOSQLite.internal().setTransactionSuccessful();
        storIOSQLite.internal().endTransaction();
    }

    @Test
    public void queryOneExistedObjectObservable() {
        final List<User> users = putUsersBlocking(3);
        final User expectedUser = users.get(0);

        final Observable<User> userObservable = storIOSQLite
                .get()
                .object(User.class)
                .withQuery(Query.builder()
                        .table(UserTableMeta.TABLE)
                        .where(UserTableMeta.COLUMN_EMAIL + "=?")
                        .whereArgs(expectedUser.email())
                        .build())
                .prepare()
                .createObservable()
                .take(1);

        TestSubscriber<User> testSubscriber = new TestSubscriber<User>();
        userObservable.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, SECONDS);
        testSubscriber.assertValue(expectedUser);
        testSubscriber.assertNoErrors();
    }

    @Test
    public void queryOneNonExistedObjectObservable() {
        putUsersBlocking(3);

        final Observable<User> userObservable = storIOSQLite
                .get()
                .object(User.class)
                .withQuery(Query.builder()
                        .table(UserTableMeta.TABLE)
                        .where(UserTableMeta.COLUMN_EMAIL + "=?")
                        .whereArgs("some arg")
                        .build())
                .prepare()
                .createObservable()
                .take(1);

        TestSubscriber<User> testSubscriber = new TestSubscriber<User>();
        userObservable.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, SECONDS);
        testSubscriber.assertValue(null);
        testSubscriber.assertNoErrors();
    }

    @Test
    public void queryOneExistedObjectTableUpdate() {
        User expectedUser = new User(null, "such@email.com");
        putUsersBlocking(3);

        final Observable<User> userObservable = storIOSQLite
                .get()
                .object(User.class)
                .withQuery(Query.builder()
                        .table(UserTableMeta.TABLE)
                        .where(UserTableMeta.COLUMN_EMAIL + "=?")
                        .whereArgs(expectedUser.email())
                        .build())
                .prepare()
                .createObservable()
                .take(2);

        TestSubscriber<User> testSubscriber = new TestSubscriber<User>();
        userObservable.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, SECONDS);
        testSubscriber.assertValue(null);
        testSubscriber.assertNoErrors();

        putUserBlocking(expectedUser);

        testSubscriber.awaitTerminalEvent(5, SECONDS);
        testSubscriber.assertValues(null, expectedUser);
        testSubscriber.assertNoErrors();
    }

    @Test
    public void queryOneNonexistedObjectTableUpdate() {
        final Observable<User> userObservable = storIOSQLite
                .get()
                .object(User.class)
                .withQuery(Query.builder()
                        .table(UserTableMeta.TABLE)
                        .where(UserTableMeta.COLUMN_EMAIL + "=?")
                        .whereArgs("some arg")
                        .build())
                .prepare()
                .createObservable()
                .take(2);

        TestSubscriber<User> testSubscriber = new TestSubscriber<User>();
        userObservable.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, SECONDS);
        testSubscriber.assertValue(null);
        testSubscriber.assertNoErrors();

        putUserBlocking();

        testSubscriber.awaitTerminalEvent(5, SECONDS);
        testSubscriber.assertValues(null, null);
        testSubscriber.assertNoErrors();
    }
}
