
Documentação perliminar do Manta-Bus
----------------------------------

* Configurar a port: especicar o argumento  -Dbus_port=####

* O ficheiro broker.wsdl especifica qual o contracto.

* Ver o ficheiro SampleMessages.txt para "templates" de mensagens que são trocados.

* Todas as mensagens começam com 4 bytes que são um número inteiro em network 
order que indica o tamanho (em bytes) da mensagem.


* TIP: "Don't 'kill -9' the Manta-Bus" se isto for feito os "shutdown hooks" não 
são efectuadas e podemos ficar num estado inconsistente.

* O encoding das mensagens deve de ser em UTF-8.

* Se o payload da mensagem for XML, este deve de ser "escapeado", e.g:
a mensagem:
	<foo>bar</foo>
deve ser passada como:
	&lt;foo&gt;bar&lt;/foo&gt;
ou no mínimo como:
	&lt;foo>bar&lt;/foo>

Em alternativa pode-se colocar o XML payload dentro de uma secção CDATA.

Have Fun!

Luis Neves
