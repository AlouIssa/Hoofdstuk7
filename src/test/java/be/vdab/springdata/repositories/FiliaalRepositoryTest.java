package be.vdab.springdata.repositories;

import be.vdab.springdata.domain.Filiaal;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;

import java.math.BigDecimal;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

@DataJpaTest
@Sql("/insertFilialen.sql")
class FiliaalRepositoryTest extends AbstractTransactionalJUnit4SpringContextTests {
    private static final String FILIALEN = "filialen";
    private final FiliaalRepository repository;
    FiliaalRepositoryTest(FiliaalRepository repository) {
        this.repository = repository;
    }

    private long idVanAlfa(){
        return jdbcTemplate.queryForObject("select id from filialen where naam='Alfa'",long.class);
    }
    private long idVanBravo(){
        return jdbcTemplate.queryForObject("select id from filialen where naam='Bravo'",long.class);
    }

    @Test
    void count(){
        assertThat(repository.count()).isEqualTo(countRowsInTable(FILIALEN));
    }

    @Test
    void findById(){
        var optionalFliaal = repository.findById(idVanAlfa());
        assertThat(optionalFliaal.get().getNaam()).isEqualTo("Alfa");
    }
    @Test
    void findAll(){
        assertThat(repository.findAll()).hasSize(countRowsInTable(FILIALEN));
    }
    @Test
    void findAllGesorteerdOpGemeente(){
        var filialen = repository.findAll(Sort.by("gemeente"));
        assertThat(filialen).hasSize(countRowsInTable(FILIALEN));
        assertThat(filialen).extracting(filiaal -> filiaal.getGemeente()).isSorted();
    }
    @Test
    void findAllById(){
        assertThat(repository.findAllById(Set.of(idVanAlfa(),idVanBravo())))
                .extracting(Filiaal::getId).containsOnly(idVanAlfa(), idVanBravo());
    }
    @Test
    void save(){
        var filiaal = new Filiaal("Delta", "Brugge" , BigDecimal.TEN);
        repository.save(filiaal);
        var id = filiaal.getId();
        assertThat(id).isPositive();
        assertThat(countRowsInTableWhere(FILIALEN,"id="+id)).isOne();
    }
    @Test
    void deleteById(){
        assertThat(repository.findAll()).hasSize(3);
        assertThat(countRowsInTable(FILIALEN)).isEqualTo(3);
        repository.deleteById(idVanAlfa());
        repository.flush();  // staat in cursus maar werkt ook zonder
        assertThat(repository.findAll()).hasSize(2);
        assertThat(countRowsInTable(FILIALEN)).isEqualTo(2);
//        assertThat(repository.findById(idVanAlfa())).isNotPresent();    // deze wilt niet lukken
//        assertThat(countRowsInTableWhere(FILIALEN, "id="+idVanAlfa())).isZero();   // deze wilt niet lukken
    }
    @Test
    void deleteByOnbestaandeId(){
        assertThatExceptionOfType(EmptyResultDataAccessException.class).isThrownBy(()->repository.deleteById(-6L));
    }
    @Test
    void findByGemeente(){
        assertThat(repository.findByGemeente("Antwerpen")).hasSize(1);
        assertThat(repository.findByGemeente("Brussel")).hasSize(2);
        assertThat(repository.findByGemeente("Antwerpen"))
                .extracting(Filiaal::getGemeente).doesNotContain("Brussel");
        assertThat(repository.findByGemeente("Brussel")).hasSize(2)
                .allSatisfy(filiaal -> assertThat(filiaal.getNaam()).isEqualTo("Brussel"));
//                .extracting(Filiaal::getGemeente).containsOnly("Brussel");
    }
    @Test
    void findByGemeenteOrderByNaam(){
        assertThat(repository.findByGemeenteOrderByNaam("Brussel")).hasSize(2)
                .allSatisfy(filiaal -> assertThat(filiaal.getGemeente()).isEqualTo("Brussel"))
                .extracting(filiaal -> filiaal.getNaam()).isSorted();

    }
    @Test
    void countByGemeente(){
        assertThat(repository.countByGemeente("Gent")).isZero();
        assertThat(repository.countByGemeente("Antwerpen")).isOne();
        assertThat(repository.countByGemeente("Brussel")).isEqualTo(2);
    }
    @Test
    void findByOmzetGreaterThanEqual(){
        var tweeduizend = BigDecimal.valueOf(2000);
        assertThat(repository.findByOmzetGreaterThanEqual(tweeduizend)).hasSize(2)
                .allSatisfy(filiaal -> assertThat(filiaal.getOmzet()).isGreaterThanOrEqualTo(tweeduizend));
    }
    @Test
    void findGemiddeldeOmzet(){
        assertThat(repository.findGemiddeldeOmzet()).isEqualByComparingTo("2000");
    }
    @Test
    void findMetHoogsteOmzet(){
        var filialen = repository.findMetHoogsteOmzet();
        assertThat(filialen).hasSize(1);
        assertThat(filialen)
                .allSatisfy(filiaal -> assertThat(filiaal.getOmzet())
                        .isEqualTo(jdbcTemplate.queryForObject("select max(f.omzet) from filialen f", BigDecimal.class)));
        assertThat(filialen)
                .allSatisfy(filiaal -> assertThat(filiaal.getOmzet())
                        .isEqualByComparingTo("3000"));
        assertThat(filialen.get(0).getNaam()).isEqualTo("Charly");
    }
}