<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet 
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" 
	xmlns:tns="http://services.sapo.pt/definitions" 
	version="1.0">
	
	<xsl:output method="xml" media-type="text/xml" indent="yes" omit-xml-declaration="yes" encoding="utf-8"/>
	
	<xsl:template match="/">
		<xsl:apply-templates select="/soap:Envelope/soap:Body/*"/>
	</xsl:template>
	
	<xsl:template match="*">
		<xsl:copy-of  select="." />
	</xsl:template>

	<!-- <xsl:template match="*">
		<xsl:element name="{local-name()}">
			<xsl:apply-templates select="node()"/>
		</xsl:element>
	</xsl:template> -->
	
</xsl:stylesheet>