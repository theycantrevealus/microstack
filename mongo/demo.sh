#!/bin/bash
./reset_log.sh && docker-compose exec -T router01 mongosh /scripts/init-router.js && ./randomizer_query.sh
