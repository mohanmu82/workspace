package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.UserAccess;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface UserAccessRepository extends JpaRepository<UserAccess, Long> {
    Optional<UserAccess> findByUserId(String userId);
    void deleteByUserId(String userId);
}
