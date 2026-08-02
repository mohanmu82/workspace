package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.model.UserAccess;
import com.mycompany.taskmanagement.store.UserAccessDataStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class UserAccessService {

    private final UserAccessDataStore dataStore;

    public List<UserAccess> list() {
        return dataStore.findAll();
    }

    public Optional<UserAccess> findByUserId(String userId) {
        return dataStore.findByUserId(userId);
    }

    public UserAccess save(String userId, UserAccess incoming) {
        UserAccess access = dataStore.findByUserId(userId).orElseGet(UserAccess::new);
        access.setUserId(userId);
        access.setCanRead(incoming.isCanRead());
        access.setCanCreate(incoming.isCanCreate());
        access.setCanUpdate(incoming.isCanUpdate());
        access.setCanDelete(incoming.isCanDelete());
        access.setActive(incoming.isActive());
        access.setAttributes(incoming.getAttributes() != null ? incoming.getAttributes() : new LinkedHashMap<>());
        return dataStore.save(access);
    }

    public void delete(String userId) {
        dataStore.deleteByUserId(userId);
    }
}
