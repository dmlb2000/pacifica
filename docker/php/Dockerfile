from php:7.3-cli

run apt-get update -y && \
    apt-get install -y apt-file && \
    apt-file update && \
    apt-get -y install \
    git libpng-dev libjpeg-dev zlib1g-dev libzip-dev libpq-dev zip \
    unzip wget ssmtp
run echo "FromLineOverride=YES" >> /etc/ssmtp/ssmtp.conf && \
    echo 'sendmail_path = "/usr/sbin/ssmtp -t"' > /usr/local/etc/php/conf.d/mail.ini
run docker-php-ext-install gd mbstring bcmath sockets zip
run docker-php-ext-install pdo pdo_mysql pdo_pgsql opcache
run EXPECTED_SIGNATURE="$(wget -q -O - https://composer.github.io/installer.sig)"; \
    php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');"; \
    ACTUAL_SIGNATURE="$(php -r "echo hash_file('sha384', 'composer-setup.php');")"; \
    if [ "$EXPECTED_SIGNATURE" != "$ACTUAL_SIGNATURE" ]; then \
        >&2 echo 'ERROR: Invalid installer signature'; \
        rm composer-setup.php; \
        exit 1; \
    fi;
run php composer-setup.php --quiet; mv composer.phar /usr/bin/composer
run mkdir /usr/src/app
workdir /usr/src/app
run composer require php-amqplib/php-amqplib guzzlehttp/guzzle:~6.0
run composer install
copy *.php ./
cmd php daemon.php

