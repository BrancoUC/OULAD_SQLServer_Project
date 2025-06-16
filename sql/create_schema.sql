--Crear Base de Datos
CREATE DATABASE oulad_db;

--Seleccionar Base de Datos
USE oulad_db;
-- Tabla 1: courses
CREATE TABLE courses (
    code_module NVARCHAR(10) NOT NULL,
    code_presentation NVARCHAR(10) NOT NULL,
    module_presentation_length INT,
    PRIMARY KEY (code_module, code_presentation)
);

-- Tabla 2: assessments
CREATE TABLE assessments (
    id_assessment INT PRIMARY KEY,
    code_module NVARCHAR(10),
    code_presentation NVARCHAR(10),
    assessment_type NVARCHAR(20),
    date INT,
    weight INT,
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
);

-- Tabla 3: vle
CREATE TABLE vle (
    id_site INT PRIMARY KEY,
    code_module NVARCHAR(10),
    code_presentation NVARCHAR(10),
    activity_type NVARCHAR(50),
    week_from INT,
    week_to INT,
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
);

-- Tabla 4: studentInfo
CREATE TABLE studentInfo (
    id_student INT PRIMARY KEY,
    code_module NVARCHAR(10),
    code_presentation NVARCHAR(10),
    gender NVARCHAR(1),
    region NVARCHAR(50),
    highest_education NVARCHAR(50),
    imd_band NVARCHAR(10),
    age_band NVARCHAR(10),
    num_of_prev_attempts INT,
    studied_credits INT,
    disability NVARCHAR(5),
    final_result NVARCHAR(45),
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
);

-- Tabla 5: studentRegistration
CREATE TABLE studentRegistration (
    id_student INT,
    code_module NVARCHAR(10),
    code_presentation NVARCHAR(10),
    date_registration INT,
    date_unregistration INT,
    PRIMARY KEY (id_student, code_module, code_presentation),
    FOREIGN KEY (id_student)
        REFERENCES studentInfo(id_student),
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation)
);

-- Tabla 6: studentAssessment
CREATE TABLE studentAssessment (
    id_assessment INT,
    id_student INT,
    date_submitted INT,
    is_banked BIT,
    score FLOAT,
    PRIMARY KEY (id_assessment, id_student),
    FOREIGN KEY (id_assessment)
        REFERENCES assessments(id_assessment),
    FOREIGN KEY (id_student)
        REFERENCES studentInfo(id_student)
);

-- Tabla 7: studentVle
CREATE TABLE studentVle (
    code_module NVARCHAR(10),
    code_presentation NVARCHAR(10),
    id_student INT,
    id_site INT,
    date INT,
    sum_click INT,
    PRIMARY KEY (code_module, code_presentation, id_student, id_site, date),
    FOREIGN KEY (code_module, code_presentation)
        REFERENCES courses(code_module, code_presentation),
    FOREIGN KEY (id_student)
        REFERENCES studentInfo(id_student),
    FOREIGN KEY (id_site)
        REFERENCES vle(id_site)
);
