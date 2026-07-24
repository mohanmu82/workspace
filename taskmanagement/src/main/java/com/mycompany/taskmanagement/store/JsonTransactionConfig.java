package com.mycompany.taskmanagement.store;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * Provides a no-op transaction manager so @Transactional annotations in TaskService
 * don't fail at startup when running with the json profile (no DataSource configured).
 */
@Configuration
@Profile("json")
public class JsonTransactionConfig {

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new AbstractPlatformTransactionManager() {
            @Override
            protected Object doGetTransaction() { return new Object(); }
            @Override
            protected void doBegin(Object tx, TransactionDefinition def) {}
            @Override
            protected void doCommit(DefaultTransactionStatus status) {}
            @Override
            protected void doRollback(DefaultTransactionStatus status) {}
        };
    }
}
