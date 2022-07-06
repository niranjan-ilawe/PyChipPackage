drop table yield.chip_runout_g;

create table yield.chip_runout_g (
    item varchar(30) null,
    descrip varchar(100) null,
    "month" varchar(20) null,
    qty numeric null
);

DROP TABLE yield.chip_mfg_g;

CREATE TABLE yield.chip_mfg_g (
	"date" varchar(20) NULL,
	wo varchar(20) NULL,
	consumed numeric NULL,
	failed numeric NULL,
	qc numeric NULL,
	inventory numeric NULL,
    "site" varchar(10) NULL
);

DROP TABLE yield.chip_mfg_error_g;

CREATE TABLE yield.chip_mfg_error_g (
	wo varchar(20) NULL,
	"date" varchar(20) NULL,
	qty numeric NULL,
	process varchar(50) NULL,
	reason varchar(100) NULL,
    "site" varchar(10) NULL
);