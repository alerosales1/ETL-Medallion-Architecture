-- Active: 1757726576592@@127.0.0.1@5432@postgres
SELECT * FROM cep_info;

SELECT * FROM products;

SELECT * FROM users;

SELECT
    u.id,
    u.nome,
    u.email,
    u.data_nascimento,
    u.genero,
    u.cep,
    cp.cep,
    cp.logradouro,
    cp.complemento,
    cp.unidade,
    cp.bairro,
    cp.localidade,
    cp.uf,
    cp.estado,
    cp.regiao,
    cp.ibge,
    cp.gia,
    cp.ddd,
    cp.siafi
FROM users AS u
    INNER JOIN cep_info AS cp ON u.cep = cp.cep