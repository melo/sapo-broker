package pt.com.broker;


public class Addressing
{
	/*
	<wsa:To>xs:anyURI</wsa:To> ?
	<wsa:From>wsa:EndpointReferenceType</wsa:From> ?
	<wsa:ReplyTo>wsa:EndpointReferenceType</wsa:ReplyTo> ?
	<wsa:FaultTo>wsa:EndpointReferenceType</wsa:FaultTo> ?
	<wsa:Action>xs:anyURI</wsa:Action>
	<wsa:MessageID>xs:anyURI</wsa:MessageID> ?
	<wsa:RelatesTo RelationshipType="xs:anyURI"?>xs:anyURI</wsa:RelatesTo> *
	<wsa:ReferenceParameters>xs:any*</wsa:ReferenceParameters> ?
	 */
	
	
	public String To;
	public String From;
	public EndpointReference ReplyTo;
	public EndpointReference FaultTo;
	public String Action;
	public String RelatesTo;
	public String ReferenceParameters;
	
	

}
