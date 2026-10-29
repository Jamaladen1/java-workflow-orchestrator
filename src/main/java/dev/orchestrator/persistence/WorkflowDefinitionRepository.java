package dev.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface WorkflowDefinitionRepository extends JpaRepository<WorkflowDefinitionEntity, UUID> {
    @Query("select coalesce(max(d.version), 0) from WorkflowDefinitionEntity d where d.name = :name")
    int maxVersion(String name);

    boolean existsByName(String name);

    List<WorkflowDefinitionEntity> findAllByOrderByNameAscVersionDesc();

    Optional<WorkflowDefinitionEntity> findFirstByNameOrderByVersionDesc(String name);
}
