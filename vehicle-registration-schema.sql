CREATE TABLE VehicleRegistration (
    Id varchar(22) primary key,
    State varchar(2) not null,
    City varchar(255) not null,
    PendingPenaltyTicketAmount decimal(5, 2),
    ValidFromDate datetime not null,
    ValidToDate datetime not null
);

CREATE TABLE Person (
    Id varchar(22) primary key,
    FirstName varchar(255) not null,
    LastName varchar(255) not null,
    DOB datetime not null,
    GovId varchar(255) not null,
    GovIdType varchar(255) not null,
    Address varchar(2000) not null
);
CREATE INDEX GovIdIdx ON Person (GovId);

CREATE TABLE VehicleOwnership (
    VehicleRegistrationIdFk varchar(22) not null,
    PersonIdFk varchar(22) not null,
    IsPrimaryOwner boolean,
    FOREIGN KEY (VehicleRegistrationIdFk) REFERENCES VehicleRegistration(Id) ON DELETE CASCADE,
    FOREIGN KEY (PersonIdFk) REFERENCES Person(Id) ON DELETE CASCADE,
    PRIMARY KEY (VehicleRegistrationIdFk, PersonIdFk)
);

CREATE TABLE DriversLicense (
    Id varchar(22) primary key,
    LicenseType varchar(255) not null,
    ValidFromDate datetime not null,
    ValidToDate datetime not null,
    PersonIdFk varchar(22),
    FOREIGN KEY (PersonIdFk) REFERENCES Person(Id) ON DELETE CASCADE
);

CREATE TABLE Vehicle (
    Id varchar(22) primary key,
    VehicleType varchar(255) not null,
    Year int not null,
    Make varchar(255) not null,
    Model varchar(255) not null,
    Color varchar(255) not null
);

CREATE TABLE VinMapping (
    VIN varchar(255) primary key,
    VehicleIdFk varchar(22),
    VehicleRegistrationIdFk varchar(22),
    FOREIGN KEY (VehicleIdFk) REFERENCES Vehicle(Id) ON DELETE RESTRICT,
    FOREIGN KEY (VehicleRegistrationIdFk) REFERENCES VehicleRegistration(Id) ON DELETE RESTRICT
);

CREATE TABLE LicensePlateNumberMapping (
    LicensePlateNumber varchar(255) primary key,
    VehicleRegistrationIdFk varchar(22),
    DriversLicenseIdFk varchar(22),
    FOREIGN KEY (VehicleRegistrationIdFk) REFERENCES VehicleRegistration(Id) ON DELETE RESTRICT,
    FOREIGN KEY (DriversLicenseIdFk) REFERENCES DriversLicense(Id) ON DELETE RESTRICT
);
