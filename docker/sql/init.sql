create database warehouse;

\c warehouse;

create table public.customers (
ID SERIAL PRIMARY KEY,
custID INT,
favourite_product VARCHAR(100),
longest_streak INT
);

insert into public.customers(custID, favourite_product, longest_streak)
values (9999999, 'foobar', 112);