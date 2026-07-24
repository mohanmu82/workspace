package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.model.Programme;
import com.mycompany.taskmanagement.service.ProgrammeService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/programmes")
@RequiredArgsConstructor
public class ProgrammeController {

    private final ProgrammeService programmeService;

    @GetMapping
    public List<Programme> list() {
        return programmeService.list();
    }

    @GetMapping("/{id}")
    public Programme getById(@PathVariable Long id) {
        return programmeService.getById(id);
    }

    @PostMapping
    public Programme create(@Valid @RequestBody Programme programme) {
        return programmeService.create(programme);
    }

    @PutMapping("/{id}")
    public Programme update(@PathVariable Long id, @Valid @RequestBody Programme programme) {
        return programmeService.update(id, programme);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        programmeService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
