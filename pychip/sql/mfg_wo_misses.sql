drop table yield.wo_misses;

create table yield.wo_misses (
    WORK_ORDER_NUMBER varchar(20) null, 
    ITEM_NAME varchar(20) null,
    ITEM_DESCRIPTION  varchar(100) null,
    MFG_AREA varchar(30) null,
    MFG_PROCESS varchar(30) null,
    CREATION_DATE date null,
    ACTUAL_START_DATE date null,
    WO_REQUIRED_DATE date null,
    WO_COMPLETION_DATE date null,
    PLANNED_START_QUANTITY numeric null,
    COMPLETED_QUANTITY numeric null,
    TIMEDIFF numeric null,
    ON_TIME varchar(20) null,
    "Reason code 1" varchar(100) null,
    "Reason code 2" varchar(100) null,
    first_pass varchar(30) null,
    final_pass varchar(30) null
);