How to run the application?

1. Configure the jms queue in standalone-full.xml, Under messaging subsystem add:

                    <address-setting match="jms.queue.testqueue">
                        <dead-letter-address>jms.queue.DLQ</dead-letter-address>
                        <expiry-address>jms.queue.ExpiryQueue</expiry-address>
                        <redelivery-delay>0</redelivery-delay>
                        <max-size-bytes>10240</max-size-bytes>
                        <page-size-bytes>2097152</page-size-bytes>
                        <address-full-policy>FAIL</address-full-policy>
                        <message-counter-history-day-limit>10</message-counter-history-day-limit>
                    </address-setting>
                    ... ...

                    <jms-queue name="testqueue">
                        <entry name="queue/testqueue"/>
                    </jms-queue

2. Start JBoss
   /usr/local/jboss-eap-6.4.5$ bin/standalone.sh -c standalone-full.xml -b 0.0.0.0 

3. Maven build the project and deploy sample-ejb-service-ear/target/sample-ejb-service-ear-1.0.1-SNAPSHOT.ear

4. http://127.0.0.1:8080/sample-ejb-service/jmstest?num=10&size=300

