CREATE FUNCTION [dbo].[udf_split] (@sep VARCHAR(32), @s VARCHAR(MAX))
RETURNS TABLE
AS
RETURN
(
    SELECT      r.value('.','VARCHAR(MAX)') as Item
    FROM        (SELECT CONVERT(XML, N'<root><r>' + REPLACE(REPLACE(REPLACE(@s,'& ','&amp; '),'<','&lt;'), @sep, '</r><r>') + '</r></root>') as valxml) x
    CROSS APPLY x.valxml.nodes('//root/r') AS RECORDS(r)
)
