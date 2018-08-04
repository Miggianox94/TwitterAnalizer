CREATE USER twitter_user IDENTIFIED BY twitter_password;
GRANT CONNECT TO twitter_user;
GRANT CONNECT, RESOURCE, DBA TO twitter_user;
GRANT UNLIMITED TABLESPACE TO twitter_user;
GRANT ALL PRIVILEGES TO twitter_user;