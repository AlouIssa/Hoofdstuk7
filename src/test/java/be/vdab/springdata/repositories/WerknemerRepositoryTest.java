package be.vdab.springdata.repositories;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;

import javax.persistence.EntityManager;

import static org.assertj.core.api.Assertions.*;

@DataJpaTest
@Sql({"/insertFilialen.sql", "/insertwerknemers.sql"})
class WerknemerRepositoryTest extends AbstractTransactionalJUnit4SpringContextTests {
    private final WerknemerRepository repository;
    private final EntityManager manager;
    WerknemerRepositoryTest(WerknemerRepository repository, EntityManager manager) {
        this.repository = repository;
        this.manager = manager;
    }
    private long idVanJoe(){
        return jdbcTemplate.queryForObject("select w.id from werknemers w where voornaam='Joe'",Long.class);
    }
    @Test
    void findByFiliaalGemeente(){
        var werknemers = repository.findByFiliaalGemeente("Brussel");
        assertThat(werknemers).hasSize(2)
                .allSatisfy(werknemer -> assertThat(werknemer.getFiliaal().getGemeente()).isEqualTo("Brussel"))
                .extracting(werknemer -> werknemer.getFiliaal().getGemeente()).doesNotContain("Antwerpen");
    }
    @Test
    void findByVoornaamStartingWith(){
        var werknemers = repository.findByVoornaamStartingWith("J");
        assertThat(werknemers).hasSize(2)
                .allSatisfy(werknemer -> assertThat(werknemer.getVoornaam())
                        .startsWith("J"));
        manager.clear();
        assertThat(werknemers).extracting(werknemer -> werknemer.getFiliaal().getNaam());
    }
    @Test
    void eerstePagina(){
        var page =repository.findAll(PageRequest.of(0,2));
        assertThat(page.getContent()).hasSize(2);
        assertThat(page.hasPrevious()).isFalse();
        assertThat(page.hasNext()).isTrue();
    }
    @Test
    void tweedePagina(){
        var page = repository.findAll(PageRequest.of(1,2));
        assertThat(page.getContent()).hasSize(1);
        assertThat(page.hasPrevious()).isTrue();
        assertThat(page.hasNext()).isFalse();
    }
    @Test
    void findAantalWerknemersPerFamilienaam(){
        var lijst = repository.findAantalWerknemersPerFamilienaam();
//        Mijn versie
        assertThat(lijst).hasSize(2);
        assertThat(lijst.get(0).getFamilienaam()).isEqualTo("Dalton");
        assertThat(lijst.get(0).getAantal()).isEqualTo(2);
        assertThat(lijst.get(1).getFamilienaam()).isEqualTo("Luke");
        assertThat(lijst.get(1).getAantal()).isEqualTo(1);

//      Versie v/d cursus
        assertThat(lijst)
                .hasSize(jdbcTemplate.queryForObject(
                        "select count(distinct familienaam) from werknemers", Integer.class))
                .filteredOn(aantalPerFamilienaam ->
                        aantalPerFamilienaam.getFamilienaam().equals("Dalton"))
                .hasSize(1)
                .element(0)
                .satisfies(aantalPerWedde -> assertThat(aantalPerWedde.getAantal())
                        .isEqualTo(countRowsInTableWhere("werknemers", "familienaam='Dalton'")));
    }
}