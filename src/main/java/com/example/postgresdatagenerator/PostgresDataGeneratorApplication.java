package com.example.postgresdatagenerator;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@SpringBootApplication
@Slf4j
public class PostgresDataGeneratorApplication implements CommandLineRunner {

    @Autowired
    private DataSourceConfig dataSourceConfig;

    private static final int USERS_AMOUNT = 1000;
    private static final int FRIENDS_LIST_SIZE = 100;

    List<String> names = List.of(
            "Mike",
            "Peter",
            "Ivan",
            "John",
            "Sergey",
            "Pavel",
            "Jacob"
    );

    public static void main(String[] args) {
        SpringApplication.run(PostgresDataGeneratorApplication.class, args);
    }

    @Override
    @SneakyThrows
    public void run(String... args) {
        try (Connection connection = dataSourceConfig.getConnection()) {
            prepareDatabase(connection);
            insertUsers(connection);
            insertFollowers(connection);
            for (int i = 1; i < 4; i++) {
                testJoinLevel(connection, i);
            }
        }
    }

    @SneakyThrows
    private void prepareDatabase(Connection connection) {
        connection.prepareStatement("""
                        DROP TABLE IF EXISTS users;
                        DROP TABLE IF EXISTS users_followers
                        """)
                .executeUpdate();
        connection.prepareStatement("""
                        create table users
                        (
                            id   bigserial primary key,
                            name varchar not null
                        );
                        create table users_followers
                        (
                            user_id   bigint,
                            followed_id bigint,
                            primary key (user_id, followed_id)
                        );
                        """)
                .executeUpdate();
        log.info("Database is loaded.");
    }

    @SneakyThrows
    private void insertUsers(Connection connection) {
        System.out.print("Insertion started");
        long start = System.currentTimeMillis();
        PreparedStatement insertUsers = connection.prepareStatement("INSERT INTO users (id, name) VALUES (?, ?)");
        for (int i = 0; i < USERS_AMOUNT; i++) {
            int index = (int) (Math.random() * (names.size() - 1));
            insertUsers.setLong(1, i + 1);
            insertUsers.setString(2, names.get(index));
            insertUsers.executeUpdate();
            if (i % (USERS_AMOUNT / 100) == 0) {
                System.out.print(".");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println();
        log.info("{} users were inserted in {} s.", USERS_AMOUNT, (end - start) / 1000.0);
    }

    @SneakyThrows
    private void insertFollowers(Connection connection) {
        System.out.print("Follower insertion started");
        long start = System.currentTimeMillis();
        PreparedStatement insertFollowers = connection.prepareStatement("INSERT INTO users_followers (user_id, followed_id) VALUES (?, ?)");
        for (int i = 0; i < USERS_AMOUNT; i++) {
            for (int j = 0; j < FRIENDS_LIST_SIZE; j++) {
                int index = (int) (Math.random() * (USERS_AMOUNT - 1));
                if (index == i) {
                    j--;
                } else {
                    insertFollowers.setLong(1, i + 1);
                    insertFollowers.setLong(2, index + 1);
                    try {
                        insertFollowers.executeUpdate();
                    } catch (SQLException e) {
                        j--;
                    }
                }
            }
            if (i % (USERS_AMOUNT / 100) == 0) {
                System.out.print(".");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println();
        log.info("{} friends were inserted in {} s.", FRIENDS_LIST_SIZE * USERS_AMOUNT, (end - start) / 1000.0);
    }

    @SneakyThrows
    private void testJoinLevel(Connection connection, int level) {
        StringBuilder sqlQuery = new StringBuilder("""
                SELECT f.id, f.name
                FROM users u
                JOIN users_followers uf1 ON uf1.user_id = u.id
                """);
        for (int i = 2; i <= level; i++) {
            sqlQuery.append("JOIN users_followers uf").append(i).append(" ON uf").append(i).append(".user_id = uf").append(i - 1).append(".followed_id\n");
        }
        sqlQuery.append("JOIN users f ON f.id = uf").append(level).append(".followed_id\n");
        sqlQuery.append("WHERE u.id = ?;");
        PreparedStatement statement = connection.prepareStatement(sqlQuery.toString());
        long start = System.currentTimeMillis();
        int index = (int) (Math.random() * (USERS_AMOUNT - 1));
        statement.setLong(1, index);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            User user = new User(resultSet.getLong("id"), resultSet.getString("name"));
        }
        long end = System.currentTimeMillis();
        log.info("Retrieve {} {} level join objects in {} s", (int) Math.pow(FRIENDS_LIST_SIZE, level), level, (end - start) / 1000.0);
    }

}
