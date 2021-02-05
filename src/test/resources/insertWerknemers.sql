insert into werknemers(voornaam, familienaam, filiaalId) VALUES
('Joe', 'Dalton', (select id from filialen where naam='Alfa')),
('Jack', 'Dalton', (select id from filialen where naam='Bravo')),
('Lucky', 'Luke', (select id from filialen where naam='Charly'));
