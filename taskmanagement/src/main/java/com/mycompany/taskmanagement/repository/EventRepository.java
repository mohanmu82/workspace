package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.Event;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EventRepository extends JpaRepository<Event, Long> {
    List<Event> findAllByOrderByStartDateAsc();
}
