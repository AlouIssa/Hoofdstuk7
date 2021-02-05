package be.vdab.springdata.repositories;

import be.vdab.springdata.domain.Werknemer;
import be.vdab.springdata.projections.AantalWerknemersPerFamilienaam;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WerknemerRepository extends JpaRepository<Werknemer, Long>{
    List<Werknemer> findByFiliaalGemeente(String gemeente);
    @EntityGraph(value = "Werknemer.metFiliaal")
    List<Werknemer> findByVoornaamStartingWith(String woord);
    Page<Werknemer> findAll(Pageable pageable);
    List<AantalWerknemersPerFamilienaam> findAantalWerknemersPerFamilienaam();
}
