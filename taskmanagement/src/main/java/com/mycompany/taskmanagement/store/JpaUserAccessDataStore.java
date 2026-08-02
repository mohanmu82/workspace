package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.UserAccess;
import com.mycompany.taskmanagement.repository.UserAccessRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@Profile("!json")
@RequiredArgsConstructor
public class JpaUserAccessDataStore implements UserAccessDataStore {

    private final UserAccessRepository userAccessRepository;

    @Override
    public List<UserAccess> findAll() {
        return userAccessRepository.findAll();
    }

    @Override
    public Optional<UserAccess> findByUserId(String userId) {
        return userAccessRepository.findByUserId(userId);
    }

    @Override
    public UserAccess save(UserAccess access) {
        return userAccessRepository.save(access);
    }

    @Override
    public void deleteByUserId(String userId) {
        userAccessRepository.deleteByUserId(userId);
    }
}
