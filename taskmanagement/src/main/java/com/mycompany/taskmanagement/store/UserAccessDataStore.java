package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.UserAccess;

import java.util.List;
import java.util.Optional;

public interface UserAccessDataStore {
    List<UserAccess> findAll();
    Optional<UserAccess> findByUserId(String userId);
    UserAccess save(UserAccess access);
    void deleteByUserId(String userId);
}
