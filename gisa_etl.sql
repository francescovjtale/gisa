drop table dataset_raw;

create table dataset_raw (
    id_controllo numeric,
    id_stabilimento numeric,
    riferimento_nome_tab text,
    id_asl numeric,
    comune text,
    provincia_stab text,
    data_inizio_attivita text,
    macroarea text,
    aggregazione text,
    attivita text,
    tipo_categorizzazione text,
    id_controllo_ultima_categorizzazione text,
    punteggio text,
    data_inizio_controllo text,
    categoria_rischio_stabilimento text,
    categoria_assegnata_da_cu text,
    categoria_ex_ante text);


COPY dataset_raw(id_controllo, id_stabilimento, riferimento_nome_tab, id_asl,comune,provincia_stab,data_inizio_attivita,macroarea,aggregazione,attivita,tipo_categorizzazione,id_controllo_ultima_categorizzazione,punteggio,data_inizio_controllo,categoria_rischio_stabilimento,categoria_assegnata_da_cu,categoria_ex_ante)
FROM '/tmp/data-1657195416141.csv'
DELIMITER ','
CSV HEADER;

update dataset_raw set categoria_assegnata_da_cu = null where upper(categoria_assegnata_da_cu) = 'NULL';
update dataset_raw set categoria_ex_ante = null where upper(categoria_ex_ante) = 'NULL';
update dataset_raw set categoria_rischio_stabilimento = null where upper(categoria_rischio_stabilimento) = 'NULL';
update dataset_raw set punteggio = null where upper(punteggio) = 'NULL';
update dataset_raw set data_inizio_attivita = null where upper(data_inizio_attivita) = 'NULL';
update dataset_raw set id_controllo_ultima_categorizzazione = null where upper(id_controllo_ultima_categorizzazione) = 'NULL';

ALTER TABLE dataset_raw
ALTER COLUMN categoria_assegnata_da_cu type numeric using categoria_assegnata_da_cu::numeric,
ALTER COLUMN categoria_ex_ante type numeric using categoria_ex_ante::numeric,
ALTER COLUMN categoria_rischio_stabilimento type numeric using categoria_rischio_stabilimento::numeric,
ALTER COLUMN punteggio type numeric using punteggio::numeric,
ALTER COLUMN data_inizio_attivita type timestamp without time zone using data_inizio_attivita::timestamp without time zone,
ALTER COLUMN id_controllo_ultima_categorizzazione type numeric using id_controllo_ultima_categorizzazione::numeric;

update dataset_raw set provincia_stab = 'benevento' where lower(provincia_stab) in ('benevento','bn');
update dataset_raw set provincia_stab = 'avellino' where lower(provincia_stab) in ('avellino','av');
update dataset_raw set provincia_stab = 'caserta' where lower(provincia_stab) in ('caserta','ce','ca');
update dataset_raw set provincia_stab = 'napoli' where lower(provincia_stab) in ('napoli','na');
update dataset_raw set provincia_stab = 'salerno' where lower(provincia_stab) in ('salerno','sa');

DELETE FROM dataset_raw WHERE provincia_stab not in ('benevento','salerno','avellino','caserta','napoli');

update dataset_raw set comune = lower(comune);

ALTER TABLE dataset_raw
ADD popolazione numeric;

update dataset_raw set popolazione = (
select comuniex.popolazione
from comuniex
where dataset_raw.comune = comuniex.comune)

ALTER TABLE dataset_gen_nc
ADD popolazione numeric;

update dataset_gen_nc set popolazione = (
select comuniex.popolazione
from comuniex
where dataset_gen_nc.comune = comuniex.comune)

ALTER TABLE dataset_gen_nc
ADD id_asl numeric;

update dataset_gen_nc set asl = (
select comuniex.asl
from comuniex
where dataset_gen_nc.comune = comuniex.comune);


alter table dataset_raw add rischio_in_aumento numeric;

update dataset_raw
set rischio_in_aumento = 1
where categoria_assegnata_da_cu > categoria_ex_ante;
update dataset_raw
set rischio_in_aumento = 0
where categoria_assegnata_da_cu <= categoria_ex_ante;

alter table dataset_raw add controllati numeric;
update dataset_raw set controllati = 1;


  update dataset_raw
  set aggregazione = 'PIATTAFORMA DI DISTRIBUZIONE ALIMENTI - NON SOG'
  WHERE macroarea = 'DEPOSITO ALIMENTI E BEVANDE CONTO TERZI NON SOGGETTO A RICONOSCIMENTO'
  and aggregazione  = 'PIATTAFORMA DI DISTRIBUZIONE ALIMENTI';

  update dataset_raw
  set aggregazione = 'COMMERCIO - FITOSAN'
  WHERE macroarea = 'PRODOTTI FITOSANITARI'
  and aggregazione  = 'COMMERCIO';

  update dataset_gen_nc
  set aggregazione = 'PIATTAFORMA DI DISTRIBUZIONE ALIMENTI - NON SOG'
  WHERE macroarea = 'DEPOSITO ALIMENTI E BEVANDE CONTO TERZI NON SOGGETTO A RICONOSCIMENTO'
  and aggregazione  = 'PIATTAFORMA DI DISTRIBUZIONE ALIMENTI';

  update dataset_gen_nc
  set aggregazione = 'COMMERCIO - FITOSAN'
  WHERE macroarea = 'PRODOTTI FITOSANITARI'
  and aggregazione  = 'COMMERCIO';

  update dataset_raw
  set aggregazione = 'RISTORAZIONE COLLETTIVA (COMUNITA ED EVENTI)'
  where aggregazione = $$RISTORAZIONE COLLETTIVA (COMUNITA' ED EVENTI)$$

  update dataset_gen_nc
  set aggregazione = 'RISTORAZIONE COLLETTIVA (COMUNITA ED EVENTI)'
  where aggregazione = $$RISTORAZIONE COLLETTIVA (COMUNITA' ED EVENTI)$$

  update dataset_raw
  set aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI CARNE, PRODOTTI A BASE DI CARNE E PREPARAZIONI DI CARNE IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI VENDITA, CONTIGUI O MENO AD ESSI'
  where aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI CARNE, PRODOTTI A BASE DI CARNE E PREPARAZIONI DI CARNE IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI VENDITA, CONTIGUI O MENO AD ESSI ';

  update dataset_gen_nc
  set aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI CARNE, PRODOTTI A BASE DI CARNE E PREPARAZIONI DI CARNE IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI VENDITA, CONTIGUI O MENO AD ESSI'
  where aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI CARNE, PRODOTTI A BASE DI CARNE E PREPARAZIONI DI CARNE IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI VENDITA, CONTIGUI O MENO AD ESSI ';

  update dataset_raw
  set aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI PRODOTTI DELLA PESCA IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI VENDITA, CONTIGUI O MENO AD ESSI'
  where aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI PRODOTTI DELLA PESCA IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI  VENDITA, CONTIGUI O MENO AD ESSI '
  or aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI PRODOTTI DELLA PESCA IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI  VENDITA, CONTIGUI O MENO AD ESSI';

  update dataset_gen_nc
  set aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI PRODOTTI DELLA PESCA IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI VENDITA, CONTIGUI O MENO AD ESSI'
  where aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI PRODOTTI DELLA PESCA IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI  VENDITA, CONTIGUI O MENO AD ESSI '
  or aggregazione = 'LAVORAZIONE E TRASFORMAZIONE DI PRODOTTI DELLA PESCA IN IMPIANTI NON RICONOSCIUTI FUNZIONALMENTE ANNESSI A ESERCIZIO DI  VENDITA, CONTIGUI O MENO AD ESSI';

  update dataset_raw
  set categoria_assegnata_da_cu = 1
  where categoria_assegnata_da_cu = 91;

  update dataset_raw
  set categoria_assegnata_da_cu = 3
  where categoria_assegnata_da_cu = 92;

  update dataset_raw
  set categoria_assegnata_da_cu = 5
  where categoria_assegnata_da_cu = 93;



  delete  from dataset_gen_nc where provincia_stab not in ('benevento','avellino','napoli','salerno','caserta')
  and asl is null or asl not in ('201','202','203','204','205','206','207');


create or replace view gisa_completo as
select id_stabilimento,comune,provincia_stab,macroarea,aggregazione,attivita,punteggio,categoria_rischio_stabilimento,categoria_assegnata_da_cu,categoria_ex_ante,popolazione,rischio_in_aumento,controllati,id_asl  from dataset_raw
union
select id_stabilimento,comune,provincia_stab,macroarea,aggregazione,attivita,null as punteggio, null as categoria_rischio_stabilimento, null as categoria_assegnata_da_cu,categoria_ex_ante,popolazione, null as rischio_in_aumento,controllati,id_asl  from dataset_gen_nc;
