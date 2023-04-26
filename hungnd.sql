CREATE OR REPLACE FUNCTION "transaction"."sm_getpromotionforso"("v_customerphonenum" varchar, "v_cartitemlist" varchar, "v_paramlist" varchar)
  RETURNS "pg_catalog"."refcursor" AS $BODY$

/*
process: Neu co bat ky 1 san pham nao da lay theo gia Sales mac dinh thi loai bi ra ko discount theo ty le phan tram


rollback transaction;
BEGIN transaction;
--select "transaction"."sm_getpromotionforso"('', '''5050GN0019NORG37'',''5050GN0019'',1,525000,525000', 'ispickup,false;source,OFF;storeid,3');
select "transaction"."sm_getpromotionforso"('0966644476', '''5051CL0008NWHT0L'',''5051CL0008'',1,645000,645000;''5052WA0077NBLK0M'',''5052WA0077'',1,565000,565000', 'ispickup,true;storeid,3;source,OFF);
FETCH ALL IN "<unnamed portal 3>";
rollback transaction;

rollback transaction;
BEGIN transaction;
--select "transaction"."sm_getpromotionforso"('', '''5050GN0019NORG37'',''5050GN0019'',1,525000,525000', 'ispickup,false;source,OFF;storeid,3');
select "transaction"."sm_getpromotionforso"('0966644476', '''5055WF0001NBLK00'',''5055WF0001'',2,425000,425000', 'ispickup,true;storeid,3;source,OFF;isutility,true');
--select "transaction"."sm_getpromotionforso"('0966644476', '''5051CL0008NWHT0L'',''5051CL0008'',1,645000,645000;''5052WA0077NBLK0M'',''5052WA0077'',1,565000,565000', 'ispickup,true;storeid,3;source,OFF;isutility,true');
FETCH ALL IN "<unnamed portal 7>";
rollback transaction;

rollback transaction;
BEGIN transaction;
--select "transaction"."sm_getpromotionforso"('', '''5050GN0019NORG37'',''5050GN0019'',1,525000,525000', 'ispickup,false;source,OFF;storeid,3');
select "transaction"."sm_getpromotionforso"('0966644476', '''5050DX0029NBLK35'',''5050DX0029'',1,445000,445000,0;''5051SD0064NWHT0S'',''5051SD0064'',1,845000,845000,0', 'ispickup,false;storeid,1000;page,cart;isutility,false');
--select "transaction"."sm_getpromotionforso"('0966644476', '''5051CL0008NWHT0L'',''5051CL0008'',1,645000,645000;''5052WA0077NBLK0M'',''5052WA0077'',1,565000,565000', 'ispickup,true;storeid,3;source,OFF;isutility,true');
FETCH ALL IN "<unnamed portal 3>";
rollback transaction;

rollback transaction;
BEGIN transaction;
--select "transaction"."sm_getpromotionforso"('', '''5050GN0019NORG37'',''5050GN0019'',1,525000,525000', 'ispickup,false;source,OFF;storeid,3');
select "transaction"."sm_getpromotionforso"('0966644476', '''5051CL0020NBEI0S'',''5051CL0020'',1,645000,451000,6245;''5051BP0048NWHT0M'',''5051BP0048'',1,725000,725000,0', 'ispickup,true;storeid,3;source,OFF;isutility,false');
--select "transaction"."sm_getpromotionforso"('0966644476', '''5051CL0008NWHT0L'',''5051CL0008'',1,645000,645000;''5052WA0077NBLK0M'',''5052WA0077'',1,565000,565000', 'ispickup,true;storeid,3;source,OFF;isutility,true');
FETCH ALL IN "<unnamed portal 33>";
rollback transaction;

rollback transaction;
BEGIN transaction;
select "transaction"."sm_getpromotionforso"('0933303988', '''5051BP0048NWHT0M'',''5051BP0048'',1,725000,725000,0;''5051HB0107NYEL0M'',''5051HB0107'',1,825000,825000,0', 'ispickup,false;storeid,1000;page,cart;isutility,false;referencecustomerid,');'ispickup,true;storeid,3;source,OFF;isutility,true');
FETCH ALL IN "<unnamed portal 1>";
rollback transaction;

rollback transaction;
BEGIN transaction;
select "transaction"."sm_getpromotionforso"('0933303988', '''5050GN0032NNAY36'',''5050GN0032'',1,495000,396000,7268;''6052WA0001NGRY0M'',''6052WA0001'',1,645000,645000,0;''5051TO0053NORG0M'',''5051TO0053'',1,715000,715000,0', 'ispickup,false;source,OFF;storeid,22;discountamount,0');
FETCH ALL IN "<unnamed portal 1>";
rollback transaction;


online
rollback transaction;
BEGIN transaction;
select "transaction"."sm_getpromotionforso1"('', '''5050MO0020NBEI36'',''5050MO0020'',1,565000,480250,8379;''5050MO0020NBEI37'',''5050MO0020'',1,565000,480250,8379', 'ispickup,false;storeid,1000;page,cart;isutility,false;discountamount,0;referencecustomerid,;includevoucheramount,0');
FETCH ALL IN "<unnamed portal 2>";
rollback transaction;

*/

DECLARE 
    v_sototalamount FLOAT8 = 0;
		v_root_sototalamount FLOAT8 = 0;
    v_sototalamount_excludedefaultsales FLOAT8 = 0;
    v_root_sototalamount_excludedefaultsales FLOAT8 = 0;
		v_sorootamount FLOAT8 = 0;
    v_discountvalue INT;
		v_minprice INT;
		v_diffpromotionamount FLOAT8 = 0;
		v_promotionamount_extra FLOAT8 = 0;
    v_promotion_discountvalue FLOAT8 = 0;
		v_promotion_discountvalue_combocount FLOAT8;
		v_vatdiscount FLOAT8 = 0;
		v_voucherapplytogethercateidamount FLOAT8 = 0;
		v_voucherapplytogethercateidamount_vip FLOAT8 = 0;--Voucher áp dụng chung với VIP
		v_voucheramount FLOAT8 = 0;
    v_promotionid INTEGER;
    v_promotionname transaction.sm_promotion.promotionname%TYPE;
    v_datefrom TIMESTAMP;
    v_dateto TIMESTAMP;
		v_utilityid INT;

		v_isvip BOOLEAN;
		v_applybirthday BOOLEAN;
		
		v_promotiontype varchar(50);
		v_promotiondetailid INT;

    --Customer:
    v_customerpoint sab_masterdata.pm_customer.customerpoint%type;
		v_customerbirthday date;
		v_customerid varchar(50);
		v_percentdiscount INT = 0;
		v_membershipid VARCHAR(20);
		v_referencecustomerid varchar(50);
		v_isusedfriendly BOOLEAN;
		v_maingroupquantity INT;
		v_maingroupquantity_gift INT;
		v_buyonegetonefree DECIMAL;
    
    v_applydefaultpromotion BOOLEAN = TRUE;
		v_applyotherpromotion BOOLEAN = TRUE;
		v_caculateoriginalpriceproduct BOOLEAN = FALSE;

    v_sql VARCHAR(8000);
    v_result boolean;
		v_socountitemsoriginalprice INT;
		v_root_socountitemsoriginalprice INT;
		v_socountitems INT;
		v_socountitems_promotion INT;
		v_usedamount INT;
		v_applyvipamount INT;
		v_applyvipamount_tmp INT;
		v_applyvipamount_extra INT;--Trừ VIP cho các sản phẩm không thuộc ngành hàng được áp dụng CTKM
		v_iscalculatevip BOOLEAN;
		v_replacevip BOOLEAN;
		
		--paramlist
		v_ispickup 			BOOLEAN;
		v_source        VARCHAR(50);
		v_page        	VARCHAR(50);
		v_promotion_source        VARCHAR(50);
		v_storeid				INT;
		v_isutility 		BOOLEAN = false;
		v_isrejectvip 	BOOLEAN = false;
		v_deliverytype	VARCHAR(50);
		v_discountamount	INT;
		v_includevoucheramount	INT;
		v_voucherlistid varchar;
		
		v_value FLOAT8;
		v_value_detail FLOAT8;
		v_cnt INT;
		v_aftervip BOOLEAN = null;

    cur_promotion CURSOR (isutility BOOLEAN) FOR
        SELECT  DISTINCT p.promotionid, p.promotionname, p.datefrom, p.dateto, p.source, u.utilityid, pd.value
        FROM    transaction.sm_promotion p
                JOIN transaction.sm_promotion_detail pd
                    ON p.promotionid = pd.promotionid
								LEFT JOIN "transaction".sm_utility u
										ON u.promotionid = p.promotionid
        WHERE   NOW() BETWEEN p.datefrom AND p.dateto
								AND (timefrom IS NULL OR NOW() :: TIME >= timefrom)
								AND (timeto IS NULL OR NOW() :: TIME <= timeto)
                AND p.isactived = TRUE
								AND p.isdeleted = false
								AND CASE WHEN isutility THEN u.utilityid IS NOT NULL ELSE u.utilityid IS NULL END
                AND (
										p.promotiontype = 'SALES'
										OR CASE WHEN POSITION('ISUTILITY,TRUE' in UPPER(v_paramlist)) > 0 THEN p.promotiontype = 'SHIP' ELSE 1 = 0 END
								)
								AND CASE WHEN POSITION('ISUTILITY,TRUE' in UPPER(v_paramlist)) > 0 THEN u.utilityid IS NOT NULL ELSE u.utilityid IS NULL END
                --AND pd.objecttypeid = 'SO'
								--AND p.promotionid in(266)---------------------
        ;

    v_item VARCHAR(8000);

    REF refcursor;
    
    
BEGIN
		CREATE TEMP TABLE tmp_parameter(
        parameterkey varchar
				,parametervalue varchar
    );
		insert into tmp_parameter(parameterkey, parametervalue)
		SELECT  SPLIT_PART(split_text, ',', 1) parameterkey, SPLIT_PART(split_text, ',', 2) parametervalue
		FROM    transaction.split_text(v_paramlist, ';');
		SELECT parametervalue	INTO v_ispickup from tmp_parameter where parameterkey = 'ispickup';
		SELECT parametervalue	INTO v_source from tmp_parameter where parameterkey = 'source';
		SELECT parametervalue	INTO v_page from tmp_parameter where parameterkey = 'page';
		SELECT parametervalue	INTO v_storeid from tmp_parameter where parameterkey = 'storeid';
		SELECT parametervalue	INTO v_isutility from tmp_parameter where parameterkey = 'isutility';
		SELECT parametervalue	INTO v_isrejectvip from tmp_parameter where parameterkey = 'rejectvip';
		SELECT parametervalue	INTO v_deliverytype from tmp_parameter where parameterkey = 'deliverytype';
		SELECT (parametervalue :: decimal)	INTO v_discountamount from tmp_parameter where parameterkey = 'discountamount';
		v_discountamount := 0;
		SELECT parametervalue	INTO v_includevoucheramount from tmp_parameter where parameterkey = 'includevoucheramount';
		SELECT parametervalue	INTO v_voucherlistid from tmp_parameter where parameterkey = 'voucherlistid';
		--Voucher 
		IF COALESCE(v_voucherlistid, '') <> '' THEN
				v_sql := '
				SELECT  SUM(price)
				FROM		sab_masterdata.pm_voucher
				WHERE   voucherid IN (''' || replace(v_voucherlistid, '-', ''',''') || ''')
								AND vouchertype NOT IN (''SNBR'', ''SNB'')
				';
				IF COALESCE(v_sql, '') <> '' THEN
						EXECUTE v_sql INTO v_voucheramount;
						raise notice 'v_sql: %', v_sql;
						raise notice 'v_voucheramount: %', v_voucheramount;
						
						v_voucheramount := COALESCE(v_voucheramount, 0);
				END IF;
		END IF;
		--End Voucher
		
		
		
		v_customerphonenum := CASE WHEN v_customerphonenum = '' THEN NULL ELSE v_customerphonenum END;
    SELECT  MAX(pc.customerpoint), MAX(pc.birthday), MAX(referencecustomerid), CASE WHEN MAX(isusedfriendly :: INT) = 0 THEN FALSE ELSE TRUE END, MAX(pc.customerid)
    INTO    v_customerpoint, v_customerbirthday, v_referencecustomerid, v_isusedfriendly, v_customerid
    FROM    sab_masterdata.pm_customer pc
    WHERE   pc.customerphonenum = v_customerphonenum;
    v_customerpoint := COALESCE(v_customerpoint, -1);
		SELECT	CASE WHEN COALESCE(v_isrejectvip, FALSE) THEN 0 ELSE percentdiscount END, CASE WHEN COALESCE(v_customerphonenum, '') = '' THEN '-' ELSE membershipid END
		INTO		v_percentdiscount, v_membershipid
		FROM		sab_masterdata.pm_customer_rangepoint
		WHERE		v_customerpoint BETWEEN rangepoint_min AND rangepoint_max;
		
		IF v_storeid = 1008 AND NOW() :: DATE = '2022-12-30' :: DATE AND v_percentdiscount >= 10 THEN
				v_percentdiscount := 5;
		END IF;
    
--    DROP TABLE IF EXISTS tmp_promotionvalues;
    CREATE TEMP TABLE tmp_promotionvalues(
        promotionid         INTEGER
				,promotiondetailid  INTEGER
        ,promotionname      VARCHAR(500)
        ,discountvalue      INT
        ,isvip              BOOLEAN
				,isapplybirthday    BOOLEAN default(false)
				,iscalculatevip			BOOLEAN
				,applydefaultpromotion BOOLEAN
				,applyotherpromotion BOOLEAN
				,productid					VARCHAR(20)
				,productname      	VARCHAR(500)
				,nameid							VARCHAR(500)
				,itemid 						VARCHAR(20)
				,itemname						VARCHAR(500)
				,maingroupid				INT
				,colorid						VARCHAR(50)
				,size								VARCHAR(50)
				,quantity						INT
				,promotiontype			VARCHAR(50)
				,usedamount 				INT
				,giftquantity				INT4 default(0)
				,image							VARCHAR(500)
				,stockquantity			INT
				,saleprice					INT8
				,retailprice				INT8
				,utilityid					INT
				,utilitytypeid			VARCHAR(50)
				,utilitytypename		VARCHAR(500)
				,remainquantity 		INT
				,utilityquantity 		INT
				,utilityusedquantity INT
				,groupapply 				varchar(50)
				,paymethod					varchar(100)
    );

--    DROP TABLE IF EXISTS tmp_cartitem;
    CREATE TEMP TABLE tmp_cartitem(
        productid       VARCHAR(50)
        ,itemid         VARCHAR(50)
        ,quantity       INT
        ,retailprice    INT
        ,saleprice      INT
				,promotionid		INT
    );
    
    SELECT  STRING_AGG('(' || split_text || ')', ' ,')
    INTO    v_item
    FROM    transaction.split_text(v_cartitemlist, ';');
		
		IF NOT EXISTS(select * from information_schema.tables where table_name = 'tmp_maingroup') THEN
				CREATE TEMP TABLE tmp_maingroup(id serial, maingroupid INT, itemid VARCHAR(50), productid VARCHAR(50),retailprice INT,saleprice INT, quantity int, usedquantity INT, usedconditionquantity INT, accumulationfromquantity INT, accumulationtoquantity INT);
		END IF;

    IF v_item IS NOT NULL THEN
        v_item := ' INSERT INTO tmp_cartitem (productid,itemid,quantity,retailprice,saleprice,promotionid) values' || v_item || ';';
        EXECUTE v_item;
				
				DELETE 	FROM tmp_cartitem
				USING		(SELECT productid, MAX(promotionid) promotionid FROM tmp_cartitem GROUP BY productid) s1
				WHERE		tmp_cartitem.productid = s1.productid AND tmp_cartitem.promotionid <> s1.promotionid
				;
				
				UPDATE	tmp_cartitem
				SET			retailprice = t.retailprice
								,saleprice = t.saleprice
				FROM		(
										SELECT	productid, GREATEST(retailprice, saleprice) retailprice, LEAST(retailprice, saleprice) saleprice
										FROM		tmp_cartitem
										--GROUP BY productid
								)t
				WHERE		t.productid = tmp_cartitem.productid;
				
        SELECT    SUM(i.quantity * i.saleprice)
									,SUM(CASE WHEN i.retailprice = i.saleprice THEN i.quantity * i.saleprice ELSE 0 END)
									,SUM(i.quantity)
									,SUM(CASE WHEN i.retailprice = i.saleprice THEN i.quantity ELSE 0 END)
									,SUM(i.quantity * i.retailprice)
									,SUM(CASE WHEN i.promotionid = 0 OR COALESCE(p.isapplysamevip, TRUE) = TRUE THEN i.quantity * i.saleprice ELSE 0 END) * v_percentdiscount / 100
        INTO      v_root_sototalamount
									,v_root_sototalamount_excludedefaultsales
									,v_socountitems
									,v_root_socountitemsoriginalprice--v_socountitemsoriginalprice
									,v_sorootamount
									,v_applyvipamount
        FROM      tmp_cartitem i
									JOIN sab_masterdata.pm_item item 
										ON item.itemid = i.itemid
									LEFT JOIN sab_masterdata.pm_promotion p
										ON p.promotionid = i.promotionid
				WHERE			(item.maingroupid IN (5050, 5051,5052,5055, 6050, 6051,6052,6055,5054))-- OR i.itemid ILIKE '%5054HW%')--, 5054
									--AND i.itemid NOT IN ('5051HB0068', '5051HB0069', '5051HB0070', '5051SD0025', '5051BP0021')
									;
    END IF;
		v_root_sototalamount := COALESCE(v_root_sototalamount, 0);
		v_root_sototalamount_excludedefaultsales := COALESCE(v_root_sototalamount_excludedefaultsales, 0);
		v_socountitems := COALESCE(v_socountitems, 0);
		v_root_socountitemsoriginalprice := COALESCE(v_root_socountitemsoriginalprice, 0);
		v_sorootamount := COALESCE(v_sorootamount, 0);
		v_applyvipamount := COALESCE(v_applyvipamount, 0);
		v_percentdiscount := COALESCE(v_percentdiscount, 0);
    
		
		v_includevoucheramount := COALESCE(v_includevoucheramount, 0);
		IF COALESCE(v_referencecustomerid, '') = '' THEN
				SELECT parametervalue	INTO v_referencecustomerid from tmp_parameter where parameterkey = 'referencecustomerid';
		END IF;
		
		v_source := COALESCE(v_source, 'ON');
		v_storeid := COALESCE(v_storeid, 0);
		IF v_storeid = 1008 AND NOW() :: DATE BETWEEN '2022-07-21' :: DATE AND '2022-08-02' :: DATE THEN
				IF (v_root_sototalamount - v_applyvipamount) >= 1299000 THEN
						v_applyvipamount := (v_root_sototalamount - v_applyvipamount) * 0.15 + v_applyvipamount;
				ELSE 
						IF (v_root_sototalamount - v_applyvipamount) >= 1299000 THEN
								v_applyvipamount := (v_root_sototalamount - v_applyvipamount) * 0.1 + v_applyvipamount;
						END IF;
				END IF;
		END IF;
		
		v_applyvipamount_tmp := v_applyvipamount;
		
		raise notice 'v_discountamount: %', COALESCE(v_discountamount, -1000);
		raise notice 'v_applyvipamount111: %', COALESCE(v_applyvipamount, -1000);
		raise notice 'v_isrejectvip: %', COALESCE(v_isrejectvip, 'false');
		--raise notice 'v_source: %', COALESCE(v_source, 'NULL-');
    
    OPEN cur_promotion(isutility := COALESCE(v_isutility, false));
    LOOP
				v_replacevip := false;
        FETCH cur_promotion INTO v_promotionid, v_promotionname, v_datefrom, v_dateto, v_promotion_source, v_utilityid, v_value_detail;
        EXIT WHEN NOT FOUND;
				raise notice '----------';
				raise notice 'v_promotionid: %', v_promotionid;
				raise notice 'v_utilityid: %', v_utilityid;
				
				TRUNCATE TABLE tmp_maingroup;
				v_aftervip := null;
				--raise notice 'v_promotionid: %', v_promotionid;
				IF EXISTS(SELECT 1 FROM "transaction".sm_promotion_condition WHERE promotionid = v_promotionid AND objecttypeid = 'PROMOTION' AND objectid = 'APPLYOTHERPROMOTION' AND operator = 'EQ' AND conditionvaluefrom = 'APPLY' AND isdeleted = false) THEN
						v_applyotherpromotion := true;
				ELSE
						v_applyotherpromotion := false;
				END IF;
				--raise notice 'v_applyotherpromotion: %', v_applyotherpromotion;
				
				--Voucher áp dụng chung cùng với CTKM
				IF EXISTS(SELECT 1 FROM "transaction".sm_promotion_condition WHERE promotionid = v_promotionid AND objecttypeid = 'VOUCHER' AND objectid = 'APPLYTOGETHERCATEID' AND operator = 'EQ' AND isdeleted = false) THEN
						v_sql := '
						SELECT  SUM(price), SUM(CASE WHEN isapplyvip THEN price ELSE 0 END)
						FROM		sab_masterdata.pm_voucher
						WHERE   voucherid IN (''' || replace(v_voucherlistid, '-', ''',''') || ''')
										AND vouchertype NOT IN (''SNBR'', ''SNB'')
										AND cateid IN (''' || replace((SELECT conditionvaluefrom FROM "transaction".sm_promotion_condition WHERE promotionid = v_promotionid AND objecttypeid = 'VOUCHER' AND objectid = 'APPLYTOGETHERCATEID' AND operator = 'EQ' AND isdeleted = false), ',', ''',''') || ''')
						';
						IF COALESCE(v_sql, '') <> '' THEN
								EXECUTE v_sql INTO v_voucherapplytogethercateidamount, v_voucherapplytogethercateidamount_vip;
								raise notice 'v_sql: %', v_sql;
								raise notice 'v_voucherapplytogethercateidamount: %', v_voucherapplytogethercateidamount;
								raise notice 'v_voucherapplytogethercateidamount_vip: %', v_voucherapplytogethercateidamount_vip;
								
								IF COALESCE(v_applyvipamount, 0) + COALESCE(v_voucherapplytogethercateidamount_vip, 0) > COALESCE(v_voucherapplytogethercateidamount, 0) THEN
										v_voucherapplytogethercateidamount := COALESCE(v_voucherapplytogethercateidamount_vip, 0);
								ELSE
										v_applyvipamount := 0;
										v_applyvipamount_tmp := v_applyvipamount;
								END IF;
								
								raise notice 'v_voucherapplytogethercateidamount: %', v_voucherapplytogethercateidamount;
								raise notice 'v_voucherapplytogethercateidamount_vip: %', v_voucherapplytogethercateidamount_vip;
								raise notice 'v_applyvipamount: %', v_applyvipamount;
						END IF;
				END IF;
				--End Voucher áp dụng chung cùng với CTKM
				
				IF (v_discountamount = 0 AND v_applyvipamount > 0 AND EXISTS(SELECT 1 FROM TRANSACTION.sm_promotion_condition WHERE promotionid = v_promotionid AND objectid = 'APPLYBIRTHDAY')) THEN
						v_discountamount := v_applyvipamount;
				END IF;
				
				v_socountitemsoriginalprice := v_root_socountitemsoriginalprice;
				v_sototalamount_excludedefaultsales := v_root_sototalamount_excludedefaultsales;
				SELECT MAX(value) INTO v_value FROM transaction.sm_promotion_detail WHERE promotionid = v_promotionid AND objecttypeid = 'SO' AND promotiontype = 'PP';
				SELECT  	SUM(CASE WHEN i.retailprice = i.saleprice THEN i.quantity * i.retailprice ELSE 0 END)
									,SUM(i.quantity)
									,SUM(i.quantity * (i.retailprice - i.saleprice))
									,SUM(CASE WHEN i.retailprice = i.saleprice THEN 0 ELSE i.quantity * (i.retailprice * v_value / 100 - (i.retailprice - i.saleprice) ) END)
									--,SUM(i.quantity * (i.retailprice * (100 - v_value) / 100)) * v_percentdiscount / 100
				INTO      v_sototalamount_excludedefaultsales
									,v_socountitemsoriginalprice
									,v_diffpromotionamount
									,v_promotionamount_extra -- Các sản phẩm có tỷ lệ giảm < tỷ lệ giảm CTKM
									--,v_applyvipamount
				FROM      tmp_cartitem i
									JOIN sab_masterdata.pm_item item 
										ON item.itemid = i.itemid
				WHERE			(item.maingroupid IN (5050, 5051,5052,5055,6050, 6051,6052,6055,5054))-- OR i.itemid ILIKE '%5054HW%')--, 5054
-- 									AND (
-- 											i.retailprice = i.saleprice 
-- 											OR COALESCE((v_value), 0) > (100 * (i.retailprice - i.saleprice) / i.retailprice)
-- 											OR(
-- 												(i.retailprice * v_value/100 + i.retailprice*(100 - v_value)/100 * (v_percentdiscount :: DECIMAL/100))--
-- 													> (i.retailprice - i.saleprice)
-- 											)
-- 									)
									;
				SELECT    SUM(CASE WHEN i.promotionid = 0 OR p.isapplysamevip = TRUE THEN i.quantity * i.saleprice ELSE i.quantity * i.retailprice END) * v_percentdiscount / 100
        INTO      v_applyvipamount_extra
        FROM      tmp_cartitem i
									JOIN sab_masterdata.pm_item item 
										ON item.itemid = i.itemid
									LEFT JOIN sab_masterdata.pm_promotion p
										ON p.promotionid = i.promotionid
				WHERE			item.maingroupid IN (5050, 5051, 5052, 5055, 6050, 6051,6052, 6055, 5054)--, 5054--Tất cả ngành hàng được áp dụng VIP
									AND item.maingroupid :: VARCHAR NOT IN (SELECT icodevalue FROM transaction.sm_promotion_icode WHERE promotionid = v_promotionid AND icodetype = 'MAINGROUP')
									AND EXISTS (SELECT icodevalue FROM transaction.sm_promotion_icode WHERE promotionid = v_promotionid AND icodetype = 'MAINGROUP')
									;
				
				raise notice 'v_sototalamount_excludedefaultsales: %', v_sototalamount_excludedefaultsales;
				raise notice 'v_diffpromotionamount: %', v_diffpromotionamount;
				raise notice 'v_percentdiscount: %', v_percentdiscount;
				raise notice 'v_applyvipamount_extra: %', v_applyvipamount_extra;
				raise notice 'v_promotionamount_extra: %', v_promotionamount_extra;
				
				IF EXISTS(
						SELECT 1
						FROM	"transaction".sm_promotion_icode
						WHERE	promotionid = v_promotionid
				) THEN
						v_sototalamount := v_root_sototalamount;
						v_sototalamount_excludedefaultsales := v_root_sototalamount_excludedefaultsales;
						
						SELECT  	SUM(i.quantity * i.saleprice)
											,SUM(CASE WHEN i.retailprice = i.saleprice THEN i.quantity * i.saleprice ELSE 0 END)
											,SUM(
													CASE 	WHEN i.retailprice = i.saleprice OR COALESCE((v_value), 0) > (100 * (i.retailprice - i.saleprice) / i.retailprice)
															THEN i.quantity 
													ELSE 0 END
											)
						INTO      v_sototalamount
											,v_sototalamount_excludedefaultsales
											,v_root_socountitemsoriginalprice
						FROM      tmp_cartitem i
											JOIN sab_masterdata.pm_item item 
												ON item.itemid = i.itemid
						WHERE			1 = 1
											AND (
												NOT EXISTS (SELECT 1 FROM "transaction".sm_promotion_icode WHERE promotionid = v_promotionid AND icodetype = 'MAINGROUP')
												OR item.maingroupid :: VARCHAR IN (SELECT icodevalue FROM "transaction".sm_promotion_icode WHERE promotionid = v_promotionid AND icodetype = 'MAINGROUP')
											)
											AND (
												NOT EXISTS (SELECT 1 FROM "transaction".sm_promotion_icode WHERE promotionid = v_promotionid AND icodetype = 'PRODUCT')
												OR item.itemid :: VARCHAR IN (SELECT icodevalue FROM "transaction".sm_promotion_icode WHERE promotionid = v_promotionid AND icodetype = 'PRODUCT')
											)
											;
				ELSE
						v_sototalamount := v_root_sototalamount;
-- 						v_sototalamount_excludedefaultsales := v_root_sototalamount_excludedefaultsales;
-- 						v_socountitemsoriginalprice := v_root_socountitemsoriginalprice;
				END IF;
				
				v_socountitemsoriginalprice := COALESCE(v_socountitemsoriginalprice, 0);
				v_vatdiscount := CASE WHEN /*v_discountamount = 0 AND*/ COALESCE(v_storeid, 0) <> 1008 AND NOW() :: DATE < '2023-01-01' :: DATE THEN v_sototalamount*(1.0-1.1/1.1) ELSE 0 END;
				raise notice 'v_vatdiscount: %', v_vatdiscount;
				
        v_discountvalue := 0;
        v_isvip = TRUE;
				v_applybirthday := false;
        v_sql := '';
        v_result := FALSE;
        v_applydefaultpromotion := TRUE;
        --check condition
        DECLARE 
            v_objecttypeid VARCHAR(50);
            v_objectid VARCHAR(50); 
            v_operator VARCHAR(50);
            v_conditionvaluefrom VARCHAR(500);
            v_conditionvalueto VARCHAR(500);
            
            
            v_condition VARCHAR(8000);
            v_socount INT;
						

            v_cur_condition CURSOR
                FOR SELECT c.objecttypeid, c.objectid, c.operator,c.conditionvaluefrom, c.conditionvalueto
                  FROM TRANSACTION.sm_promotion_condition c
                  WHERE  C.promotionid = v_promotionid
													 AND isdeleted = false;
        BEGIN
            v_iscalculatevip := true;
            OPEN v_cur_condition;
            LOOP
                FETCH v_cur_condition INTO v_objecttypeid, v_objectid, v_operator, v_conditionvaluefrom, v_conditionvalueto;
                EXIT WHEN NOT FOUND;
								v_condition := NULL;
-- 								raise notice 'v_applyotherpromotion: %', v_applyotherpromotion;
                IF v_objecttypeid = 'CUSTOMER' THEN
                    IF v_objectid IN ('MAXSOCOUNT', 'SOCOUNTORDERNUM') THEN
                        SELECT  SUM(Total)
                        INTO    v_socount
                        FROM (
                            SELECT  COUNT(1) Total
                            FROM    transaction.pm_saleorder 
                            WHERE   customerphone = v_customerphonenum 
                                    AND outproductstatus <> 5 
                                    AND orderdate BETWEEN v_datefrom AND v_dateto
                                    AND isdeleted = FALSE
                            UNION ALL
                            SELECT  COUNT(1) Total
                            FROM    transaction.pm_outputvoucher po
                            WHERE   po.customerphonenum = v_customerphonenum 
                                    AND po.createddate BETWEEN v_datefrom AND v_dateto
                                    AND saleorderid IS NULL
                                    AND po.isdeleted = FALSE
                        ) SO
                        ;
                        raise notice 'v_socount: %', COALESCE(v_socount, '-1');
                    END IF;

                    IF v_objectid = 'VIP' AND v_conditionvaluefrom = 'VIPORMEMBER' THEN 
                        --v_condition := CASE WHEN v_operator = 'EQ' THEN v_customerpoint || ' > 0' ELSE '' END;
                        v_isvip = v_isvip;
										ELSIF v_objectid = 'VIP' AND v_conditionvaluefrom IN ('VIPCONDITION') THEN 
                       v_condition := CASE WHEN v_operator = 'EQ' THEN v_customerpoint || ' >= 250' ELSE '' END;
											 raise notice 'v_condition_VIPCONDITION: %', COALESCE(v_condition, 'NULL');
											 raise notice 'v_customerpoint: %', COALESCE(v_customerpoint, NULL);
                    ELSIF v_objectid = 'VIP' AND v_conditionvaluefrom IN ('MEMBERCONDITION') THEN 
                       v_condition := CASE WHEN v_operator = 'EQ' THEN v_customerpoint || ' < 250' ELSE '' END;
										ELSIF v_objectid = 'VIP' AND v_conditionvaluefrom IN ('NOTVIP') THEN --('NOTVIPORMEMBER', 'NOTVIP')
                       --v_condition := CASE WHEN v_operator = 'EQ' THEN v_customerpoint || ' <= 0' ELSE '' END;
                       v_isvip = FALSE;
									 	ELSIF v_objectid = 'VIP' AND v_conditionvaluefrom IN ('NOTVIPORMEMBER') THEN 
												v_condition := CASE WHEN v_operator = 'EQ' THEN COALESCE(v_customerpoint, -1) || ' = 0' ELSE '' END;
												raise notice 'loichonay: %', COALESCE(v_customerpoint, '-1');
										ELSIF v_objectid = 'VIP' AND v_conditionvaluefrom IN ('VIPORMEMBERCONDITION') THEN 
													v_condition := CASE WHEN v_operator = 'EQ' THEN COALESCE(v_customerpoint, 0) || ' BETWEEN 0 AND 249 ' ELSE '' END;
-- 													v_condition := CASE WHEN v_operator = 'EQ' THEN COALESCE(v_customerpoint, 0) || ' >= 0' ELSE '' END;
													--raise notice 'v_condition: %', COALESCE(v_operator, 'NULL');
                    ELSIF v_objectid = 'VIP' AND v_conditionvaluefrom IN ('AFTERVIP', 'BEFOREVIP') AND NOT EXISTS(SELECT 1 FROM "transaction".sm_promotion_detail WHERE promotionid = v_promotionid AND promotiontype = 'GIFT') THEN 
													v_iscalculatevip := false;
													v_aftervip := CASE WHEN v_conditionvaluefrom = 'AFTERVIP' THEN true ELSE false END;
													IF EXISTS(SELECT 1 FROM "transaction".sm_promotion_condition WHERE promotionid = v_promotionid AND objecttypeid = 'CUSTOMER' AND objectid = 'REPLACEVIP' AND isdeleted = false) THEN
															v_replacevip := true;
													END IF;
										ELSIF v_objectid = 'APPLYBIRTHDAY' AND v_conditionvaluefrom IN ('TRUE') THEN 
                        v_applybirthday = TRUE;
										ELSIF v_objectid = 'FRIENDLYVIP' THEN
													raise notice 'v_referencecustomerid: %', v_referencecustomerid;
													raise notice 'v_customerphonenum: %', v_customerphonenum;
													raise notice 'v_customerpoint: %', v_customerpoint;
													IF  COALESCE(v_referencecustomerid, '') <> '' AND v_isusedfriendly = FALSE AND v_customerphonenum <> '' 
															AND EXISTS(
																	SELECT 	1 
																	FROM 		sab_masterdata.pm_customer cus 
																					JOIN sab_masterdata.pm_customer_rangepoint cr 
																							ON cus.customerpoint BETWEEN cr.rangepoint_min AND cr.rangepoint_max
																	WHERE		cus.customerid = v_referencecustomerid
																					AND cr.membershipid = v_conditionvaluefrom
															)
													THEN 
															IF NOT EXISTS(SELECT 1 FROM TRANSACTION.pm_saleorder WHERE customerphone = v_customerphonenum AND isdeleted = false) THEN
																	v_condition := '''FRIENDLYVIP'' = ''FRIENDLYVIP''';
															END IF;
													END IF;
													v_condition := COALESCE(v_condition, '''FRIENDLYVIP'' <> ''FRIENDLYVIP''');
										ELSIF v_objectid = 'MAXSOCOUNT' THEN
                        v_condition := CASE WHEN v_operator = 'EQ' THEN COALESCE(v_socount, 0) || ' < ' || v_conditionvaluefrom ELSE '' END;
                    ELSIF v_objectid = 'SOCOUNTORDERNUM' THEN
                        v_condition := CASE WHEN v_operator = 'EQ' THEN COALESCE(v_socount, 0) + 1 || ' = ' || v_conditionvaluefrom ELSE '' END;
										ELSIF v_objectid = 'BIRTHDAY' THEN
                        v_condition := 	CASE 	WHEN 'BIRTHDAYONMONTH' = v_conditionvaluefrom AND EXTRACT('MONTH' from v_customerbirthday) = EXTRACT('MONTH' from NOW()) THEN '''BIRTHDAY'' = ''BIRTHDAY''' 
																							ELSE '''BIRTHDAY'' = ''' || COALESCE(EXTRACT('MONTH' from v_customerbirthday) :: VARCHAR(50), 'NULL') || ''''
																				END;
                    ELSIF v_objectid = 'MEMBERSHIPID' THEN
                        v_condition := 	'''' || v_membershipid || '''' || CASE 	WHEN v_operator = 'EQ' THEN ' = ''' || v_conditionvaluefrom || ''''
																																								WHEN v_operator = 'BW' THEN ' BETWEEN ''' || v_conditionvaluefrom || ''' AND ''' || v_conditionvalueto || ''''
																																								ELSE '' 
																																					END;
										END IF;
                ELSEIF v_objecttypeid = 'PROMOTION' THEN
                    IF v_objectid = 'DEFAULTPROMOTION' AND v_conditionvaluefrom = 'DONOTAPPLY' THEN
                        v_applydefaultpromotion = FALSE;
                    END IF;
								ELSEIF v_objecttypeid = 'SO' THEN
                    IF v_objectid = 'RECEIVETYPE' AND v_conditionvaluefrom = 'STORE' THEN
                        v_condition := v_ispickup || ' = true ';
										ELSEIF v_objectid = 'SOCOUNTITEMS' THEN
												v_socountitems := COALESCE(v_socountitems, 0);
                        v_condition := v_socountitems || CASE WHEN v_operator = 'EQ' THEN ' = ' WHEN v_operator = 'GE' THEN ' >= ' ELSE '' END || v_conditionvaluefrom;
												v_socountitems_promotion := v_conditionvaluefrom;
										ELSEIF v_objectid = 'DELIVERYTYPE' THEN
                        v_condition := v_deliverytype || CASE WHEN v_operator = 'EQ' THEN ' = ' WHEN v_operator = 'NEQ' THEN ' <> ' WHEN v_operator = 'GE' THEN ' >= ' ELSE '' END || v_conditionvaluefrom;
										ELSEIF v_objectid = 'SOCOUNTITEMSORIGINALPRICE' THEN
                        v_condition := v_socountitemsoriginalprice || CASE WHEN v_operator = 'EQ' THEN ' = ' WHEN v_operator = 'GE' THEN ' >= ' ELSE '' END || v_conditionvaluefrom;
												v_caculateoriginalpriceproduct := true;
                    ELSEIF v_objectid = 'FIRSTSO' THEN
                        v_condition := CASE WHEN COALESCE(v_customerphonenum, '') = '' THEN ' 1 = 0 ' ELSE ' NOT EXISTS(SELECT 1 FROM "transaction".pm_saleorder WHERE outproductstatus IN (1,2,6) AND customerphone = ''' || v_customerphonenum || ''') ' END;
										ELSEIF v_objectid = 'SOROOTAMOUNT' THEN
                        --raise notice 'v_sorootamount: %', COALESCE(v_sorootamount, -1);
												v_condition := COALESCE(v_sorootamount - CASE WHEN EXISTS(SELECT 1 FROM "transaction".sm_promotion_condition WHERE promotionid = v_promotionid AND objectid ='VIP' AND conditionvaluefrom = 'AFTERVIP' AND isdeleted = false) THEN v_applyvipamount ELSE 0 END
																																		 || CASE 	WHEN v_operator = 'EQ' THEN ' = ' || v_conditionvaluefrom 
																																				WHEN v_operator = 'GE' THEN ' >= ' || v_conditionvaluefrom
																																				WHEN v_operator = 'BW' THEN ' BETWEEN ' || v_conditionvaluefrom || ' AND ' || v_conditionvalueto
																																				ELSE '' 
																																				END, ' 1 = 0 ') ;
												--raise notice 'v_sorootamount: %', COALESCE(v_sorootamount, -1);
												v_usedamount := v_conditionvaluefrom;
										ELSEIF v_objectid IN ('SOAMOUNTAFTERDISCOUNT', 'SOAMOUNTAFTERDISCOUNTEXCLUDEDEFAULTSALES') THEN
												v_applyvipamount := v_applyvipamount_tmp;
												IF v_objectid IN ('SOAMOUNTAFTERDISCOUNTEXCLUDEDEFAULTSALES') THEN
														v_caculateoriginalpriceproduct = true;
												END IF;
                        v_discountamount := CASE WHEN v_discountamount <= COALESCE(v_applyvipamount, 0) THEN 0 ELSE v_discountamount END;
												v_condition := CASE WHEN V_OBJECTID = 'SOAMOUNTAFTERDISCOUNT' THEN v_sototalamount - COALESCE(v_applyvipamount, 0) - (CASE WHEN v_source = 'OFF' OR v_storeid <> 1000 THEN 0 ELSE COALESCE(v_discountamount, 0) END) - COALESCE(v_vatdiscount, 0) ELSE v_sototalamount_excludedefaultsales END
																													+ CASE WHEN EXISTS(SELECT 1 FROM transaction.sm_promotion_condition WHERE promotionid = v_promotionid AND objectid = 'VIP' AND conditionvaluefrom ='AFTERVIP' AND isdeleted = false) THEN COALESCE(v_applyvipamount, 0) ELSE 0 END
																													- COALESCE(v_voucherapplytogethercateidamount, 0)
																													|| CASE WHEN v_operator = 'EQ' THEN ' = ' || v_conditionvaluefrom 
																																	WHEN v_operator = 'GE' THEN ' >= ' || v_conditionvaluefrom 
																																	WHEN v_operator = 'BW' THEN ' BETWEEN ' || v_conditionvaluefrom || ' AND ' || v_conditionvalueto
																																	ELSE ''
																														END
																													;
												v_usedamount := v_conditionvaluefrom;
												raise notice 'v_condition: %', COALESCE(v_condition, 'sablanca - null');
												raise notice 'v_usedamount: %', COALESCE(v_usedamount, -1);
												raise notice 'v_sototalamount: %', COALESCE(v_sototalamount, -1);
												raise notice 'v_applyvipamount999: %', COALESCE(v_applyvipamount, -1);
												raise notice 'v_storeid: %', COALESCE(v_storeid, -1);
												
												EXECUTE 'select ' || v_condition INTO v_result;
												IF v_storeid = 1000 AND v_result = false AND EXISTS(SELECT 1 FROM transaction.sm_promotion_condition WHERE promotionid = v_promotionid AND objectid = 'VIP' AND conditionvaluefrom ='BEFOREVIP' AND isdeleted = false) THEN
														v_applyvipamount := 0;
														v_discountamount := CASE WHEN v_discountamount <= COALESCE(v_applyvipamount, 0) THEN 0 ELSE v_discountamount END;
														v_condition := CASE WHEN V_OBJECTID = 'SOAMOUNTAFTERDISCOUNT' THEN v_sototalamount - COALESCE(v_applyvipamount, 0) - (CASE WHEN v_source = 'OFF' OR v_storeid <> 1000 THEN 0 ELSE COALESCE(v_discountamount, 0) END) - COALESCE(v_vatdiscount, 0) ELSE v_sototalamount_excludedefaultsales END
																															+ CASE WHEN EXISTS(SELECT 1 FROM transaction.sm_promotion_condition WHERE promotionid = v_promotionid AND objectid = 'VIP' AND conditionvaluefrom ='AFTERVIP' AND isdeleted = false) THEN COALESCE(v_applyvipamount, 0) ELSE 0 END
																															- COALESCE(v_voucherapplytogethercateidamount, 0)
																															|| CASE WHEN v_operator = 'EQ' THEN ' = ' || v_conditionvaluefrom 
																																			WHEN v_operator = 'GE' THEN ' >= ' || v_conditionvaluefrom 
																																			WHEN v_operator = 'BW' THEN ' BETWEEN ' || v_conditionvaluefrom || ' AND ' || v_conditionvalueto
																																			ELSE ''
																																END
																															;
														raise notice 'v_condition time 2: %', COALESCE(v_condition, 'sablanca - null');
												END IF;
										END IF;
								ELSEIF v_objecttypeid = 'SOPRODUCT' THEN
                    IF v_objectid = 'MAINGROUP' THEN
                        v_condition := ' EXISTS (SELECT 1 FROM tmp_cartitem t join sab_masterdata.pm_product p ON p.productid = t.productid join sab_masterdata.pm_item i on i.itemid = p.itemid where i.maingroupid IN (' || v_conditionvaluefrom || ')) ';
										ELSEIF v_objectid = 'ITEMID' THEN
                        v_condition := ' EXISTS (SELECT 1 FROM tmp_cartitem WHERE itemid IN (''' || replace(v_conditionvaluefrom, ',', ''',''') || ''')) ';
                    ELSEIF v_objectid = 'ACCESSORYFORMAINGROUP' THEN
                        --v_condition := ' EXISTS (SELECT 1 FROM tmp_cartitem t join sab_masterdata.pm_item i on i.itemid = t.itemid where i.maingroupid = ' || v_conditionvaluefrom || ') ';
												SELECT	string_agg('EXISTS (SELECT 1 FROM tmp_cartitem t join sab_masterdata.pm_item i on i.itemid = t.itemid where i.maingroupid = ' || conditionvaluefrom || ')', ' OR ')
												INTO		v_condition
												FROM		transaction.sm_promotion_condition
												WHERE		promotionid = v_promotionid
																AND objecttypeid = 'SOPRODUCT'
																AND objectid = 'ACCESSORYFORMAINGROUP';
												v_condition := '(' || v_condition || ')';
												
-- 												SELECT	(v_root_sototalamount - 1000) / SUM(t.quantity)
-- 												INTO		v_usedamount
-- 												FROM		tmp_cartitem t join sab_masterdata.pm_item i on i.itemid = t.itemid 
-- 												WHERE		i.maingroupid :: varchar = v_conditionvaluefrom;
												
										END IF;
										
										IF v_conditionvaluefrom IS NOT NULL AND v_objectid NOT IN ('ACCESSORYFORMAINGROUP') THEN
												INSERT INTO tmp_maingroup(maingroupid, itemid, productid, retailprice, saleprice, quantity, usedquantity)
												SELECT		i.maingroupid, i.itemid, t.productid, t.retailprice, t.saleprice, t.quantity, 0
												FROM			tmp_cartitem t
																	LEFT JOIN sab_masterdata.pm_product p ON p.productid = t.productid 
																	LEFT JOIN sab_masterdata.pm_item i on i.itemid = p.itemid
												ORDER BY	CASE WHEN i.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ',')) OR i.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ',')) THEN 1 ELSE 0 END, t.saleprice DESC
												;
												SELECT SUM(quantity)
												INTO		v_maingroupquantity
												FROM 		tmp_maingroup t
												WHERE 	t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
												;
												
												UPDATE	tmp_maingroup
												SET			accumulationfromquantity = COALESCE(
																		(		SELECT 	SUM(t.quantity) 
																				FROM 		tmp_maingroup t 
																				WHERE 	(t.id < tmp_maingroup.id OR (t.id = tmp_maingroup.id AND v_conditionvaluefrom = v_conditionvalueto))
																								AND (
																										t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																										OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																								)
																		)
																, 0)
												WHERE 	tmp_maingroup.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																OR tmp_maingroup.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
												;
												
												UPDATE	tmp_maingroup
												SET			accumulationtoquantity = COALESCE(
																		(		SELECT 	SUM(t.quantity) 
																				FROM 		tmp_maingroup t 
																				WHERE 	(t.id < tmp_maingroup.id OR (t.id = tmp_maingroup.id AND v_conditionvaluefrom = v_conditionvalueto))
																								AND (
																										t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																										OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																								)
																		)
																, 0)
												WHERE 	tmp_maingroup.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																OR tmp_maingroup.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
												;
												
												UPDATE	tmp_maingroup 
												SET 		usedquantity = CASE WHEN accumulationtoquantity + quantity <= accumulationfromquantity THEN quantity ELSE accumulationfromquantity - accumulationtoquantity END
												WHERE 	(
																		tmp_maingroup.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																		OR tmp_maingroup.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																)
																AND v_conditionvaluefrom <> v_conditionvalueto
-- 																AND COALESCE(
-- 																		(		SELECT 	SUM(t.quantity) 
-- 																				FROM 		tmp_maingroup t 
-- 																				WHERE 	t.id < tmp_maingroup.id
-- 																								AND t.maingroupid IN (SELECT split_text :: INT FROM transaction.split_text(v_conditionvalueto, ','))
-- 																		)
-- 																, 0) <= div(v_maingroupquantity, (CASE WHEN v_conditionvaluefrom = v_conditionvalueto THEN 2 ELSE 1 END))
																;
												UPDATE	tmp_maingroup 
												SET 		usedquantity = "floor"(CASE WHEN COALESCE(
																																(
																																		SELECT 	SUM(t.quantity) 
																																		FROM 		tmp_maingroup t 
																																		WHERE 	t.id < tmp_maingroup.id
																																						AND (
																																								t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																																								OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																																						)
																																)
																													, 0) % 2 = 1 THEN 1 ELSE 0 END + quantity :: DECIMAL
																													/ 2 :: DECIMAL)
												--SET 		usedquantity = "floor"(10 :: numeric / 2 :: numeric)
												WHERE 	v_conditionvaluefrom = v_conditionvalueto
																AND (
																		tmp_maingroup.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																		OR tmp_maingroup.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																)
												;
												UPDATE	tmp_maingroup 
												SET 		usedconditionquantity = CASE WHEN (
																																COALESCE(
																																		(		SELECT 	SUM(t.quantity) 
																																				FROM 		tmp_maingroup t 
																																				WHERE 	t.id < tmp_maingroup.id
																																								AND (
																																										t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																										OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																								)
																																		)
																																, 0)
																														) + quantity <= COALESCE((SELECT SUM(usedquantity) FROM tmp_maingroup), 0)--SL duoc tang
																														THEN quantity
																												ELSE 		GREATEST(
																																		COALESCE((SELECT SUM(usedquantity) FROM tmp_maingroup), 0) - --SL duoc tang
																																		COALESCE(
																																				(		SELECT 	SUM(t.quantity) 
																																						FROM 		tmp_maingroup t 
																																						WHERE 	t.id < tmp_maingroup.id
																																										AND (
																																												t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																												OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																										)
																																				)
																																		, 0)--SL con lai 
																																, 0)
																															
																												END
												WHERE 	tmp_maingroup.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																OR tmp_maingroup.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																;
												raise notice 'v_maingroupquantity: %', COALESCE(v_maingroupquantity, 0);
												raise notice 'v_value_detail for mua giày tặng ví: %', COALESCE(v_value_detail, 0);
												
												
-- 												OPEN REF
-- 												FOR SELECT  *
-- 																, v_maingroupquantity, v_conditionvaluefrom, v_conditionvalueto, v_promotionid
-- 																,div(v_maingroupquantity, (CASE WHEN v_conditionvaluefrom = v_conditionvalueto THEN 2 ELSE 1 END))
-- 														FROM    tmp_maingroup;
-- 												RETURN REF;
												
												IF (SELECT SUM(usedquantity) FROM tmp_maingroup) = v_maingroupquantity THEN
														SELECT SUM(usedquantity * (retailprice - CASE WHEN v_value_detail < 100 THEN (retailprice * (100 - v_value_detail)) / 100 ELSE v_value_detail END))
														INTO		v_buyonegetonefree
														FROM		tmp_maingroup
														WHERE		usedquantity > 0;
														--DELETE FROM tmp_maingroup WHERE maingroupid IN (SELECT split_text :: INT FROM transaction.split_text(v_conditionvaluefrom, ','));
												ELSE--v_maingroupquantity nhỏ hơn số lượng hàng tặng
														v_maingroupquantity := GREATEST((SELECT SUM(usedquantity) FROM tmp_maingroup) - v_maingroupquantity, 0);--Số lượng sản phẩm dư ra
														SELECT SUM(usedquantity * (retailprice - CASE WHEN v_value_detail < 100 THEN (retailprice * (100 - v_value_detail)) / 100 ELSE v_value_detail END)) - MIN((retailprice - CASE WHEN v_value_detail < 100 THEN (saleprice * (100 - v_value_detail)) / 100 ELSE v_value_detail END) * v_maingroupquantity)
														INTO		v_buyonegetonefree
														FROM		tmp_maingroup
														WHERE		usedquantity > 0;
												END IF;
-- 												OPEN REF FOR select *, v_buyonegetonefree, v_maingroupquantity
-- 												FROM		tmp_maingroup
-- 												;
-- 												RETURN REF;
												
												raise notice 'v_buyonegetonefree: %', COALESCE(v_buyonegetonefree, 0);
												SELECT SUM(quantity)
												INTO		v_maingroupquantity
												FROM 		tmp_maingroup t
												WHERE 	t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
												;
												
												SELECT SUM(quantity)
												INTO		v_maingroupquantity_gift
												FROM 		tmp_maingroup t
												WHERE 	t.maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																OR t.itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
												;
												raise notice 'v_maingroupquantity: %', COALESCE(v_maingroupquantity, 0);
												raise notice 'v_maingroupquantity_gift: %', COALESCE(v_maingroupquantity_gift, 0);
												raise notice 'v_buyonegetonefree: %', COALESCE(v_buyonegetonefree, 0);
												--Lấy thêm phần Sale mặc định
												v_buyonegetonefree := v_buyonegetonefree 
																																	+ COALESCE(
																																						(
																																								SELECT	SUM((retailprice - saleprice) * quantity)
																																								FROM		tmp_maingroup
																																								WHERE		usedquantity = 0 -- Sản phẩm ko thuộc ngành hàng tặng hay ngành hàng ĐK hoặc sản phẩm dư ra
																																												AND maingroupid :: varchar NOT IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																												AND maingroupid :: varchar NOT IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																																												AND itemid NOT IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																												AND itemid NOT IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																																						), 0)
																																	--Lây sản phẩm túi mà có Sale MĐ sẽ phải trừ phần sản phẩm đã giảm giá
																																	- COALESCE(
																																			(
																																					SELECT	SUM((retailprice - saleprice) * usedconditionquantity)
																																					FROM		tmp_maingroup
																																					WHERE		usedquantity = 0
																																									AND (
																																											maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																											OR itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																									)
																																			)
																																	, 0)
																																	--Lấy danh sách giảm thêm VIP khi có CTKM mua 1 được 1 : Sản phẩm điều kiện
																																	+ COALESCE(
																																			(
																																					SELECT	SUM((retailprice * quantity * v_percentdiscount / 100) - (retailprice - saleprice) )
																																					FROM		tmp_maingroup
																																					WHERE		EXISTS(SELECT 1 FROM transaction.sm_promotion_condition WHERE promotionid = v_promotionid AND objectid = 'VIP' AND objecttypeid = 'CUSTOMER' AND conditionvaluefrom = 'VIPFORCONDITIONPRODUCT')
																																									AND (
																																											maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																											OR itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvaluefrom, ','))
																																									)
																																									AND 100 * (retailprice - saleprice) / retailprice < v_percentdiscount
																																									AND usedquantity = 0
																																			)
																																	, 0)
																																	--Lấy danh sách giảm thêm VIP khi có CTKM mua 1 được 1 : Sản phẩm tặng dư ra
																																	+ COALESCE(
																																			(
																																					SELECT	SUM((retailprice * (quantity - usedquantity) * v_percentdiscount / 100 - (retailprice - saleprice) ))
																																					FROM		tmp_maingroup
																																					WHERE		EXISTS(SELECT 1 FROM transaction.sm_promotion_condition WHERE promotionid = v_promotionid AND objectid = 'VIP' AND objecttypeid = 'CUSTOMER' AND conditionvaluefrom = 'VIPFORCONDITIONPRODUCT')
																																									AND (
																																											maingroupid :: varchar IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																																											OR itemid IN (SELECT split_text FROM transaction.split_text(v_conditionvalueto, ','))
																																									)
																																									AND 100 * (retailprice - saleprice) / retailprice < v_percentdiscount
																																									AND quantity - usedquantity > 0
																																			)
																																	, 0)
													;
												
-- 												OPEN REF
-- 												FOR SELECT  *
-- 																, v_maingroupquantity, v_conditionvaluefrom, v_conditionvalueto, v_promotionid
-- 																,v_buyonegetonefree
-- 														FROM    tmp_maingroup;
-- 												RETURN REF;
												
												raise notice 'v_buyonegetonefree: %', COALESCE(v_buyonegetonefree, 0);
												raise notice 'v_applyvipamount aaa: %', COALESCE(v_applyvipamount, 0);
												
-- 												open ref
-- 												for select  * , v_maingroupquantity, v_maingroupquantity_gift, v_buyonegetonefree AAAAA
-- 														from    tmp_maingroup t
-- 														order by id;
-- 												return ref;
										END IF;
								END IF;
                
								
                --raise notice 'v_condition: %', COALESCE(v_condition, 'NULL');
--                raise notice 'v_objecttypeid: %', COALESCE(v_objecttypeid, 'NULL');
--                raise notice 'v_objectid: %', COALESCE(v_objectid, 'NULL');
    
                v_sql := v_sql || COALESCE(' AND ' || v_condition, '');
            END LOOP;
						CLOSE v_cur_condition;
        END;
				
        --raise notice 'v_promotion_source: %', COALESCE(v_promotion_source, 'NULL--');
				--raise notice 'v_source: %', COALESCE(v_source, 'NULL--');
				IF v_promotion_source IS NOT NULL THEN
					v_sql := v_sql || ' AND ''' || v_promotion_source || ''' = ''' || v_source || '''';
				END IF;
				IF EXISTS(SELECT 1 FROM transaction.sm_promotion_store WHERE promotionid = v_promotionid) 
						AND NOT EXISTS(SELECT 1 FROM transaction.sm_promotion_store WHERE promotionid = v_promotionid and storeid = v_storeid)
						THEN
					v_sql := v_sql || ' AND 1 = 0 ';
				END IF;
				
        v_sql := '
        SELECT  true
        WHERE   (1 = 1) ' || v_sql || '';
        EXECUTE v_sql INTO v_result;
        
        raise notice 'v_sql: %', COALESCE(v_sql, 'NULL--');
        raise notice 'v_result: %', v_result;
				--raise notice 'v_promotionid: %', v_promotionid;
        --End check condition
        
				
				
        IF v_result THEN
						DECLARE 
							v_groupidcount int;
							--v_combonumber INT = 0;
						BEGIN
							v_promotion_discountvalue := 0;
							SELECT  promotiontype, value, pd.promotiondetailid
							INTO    v_promotiontype, v_value, v_promotiondetailid
							FROM    transaction.sm_promotion_detail pd
							WHERE   pd.promotionid = v_promotionid
											AND pd.objecttypeid = 'SO' AND pd.objectid = 'ALL';
							IF v_promotiontype IN ('COMBO', 'COMBO_PP', 'COMBOLIST_PP') THEN
									/* Giảit thuật
										- Nếu promotion là combo thì get all group của 1 promotion. Nếu số group của sản phẩm trong giỏ hàng < số lượng group của CTKM thì ko được hưởng KM
										- SL setup,Giá bán, SL mua 
											=> Chia lấy phần nguyên SL mua cho SL setup 
											=> MIN(Chia lấy phần nguyên của all sp): Bộ combo
											=> SL tham gia vào bộ Combo của từng SP = Bộ combo * số lượng Setup của tưng SP
											=> Giá trị giảm giá của Promotion = Sum(Giá bán * SL tham gia vào Combo) - Giá trị của Combo
									*/
									v_promotion_discountvalue_combocount := 0;
									
									DROP TABLE IF EXISTS tmp_comboitem;
									CREATE TEMP TABLE tmp_comboitem(
											itemid         	VARCHAR(50)
											,salequantity     INT
											,comboquantity 		INT
											,retailprice    INT
											,saleprice      	INT
											,groupid					VARCHAR(50)
											
									);
									INSERT INTO tmp_comboitem(itemid, salequantity, comboquantity, retailprice, saleprice, groupid)
									SELECT	ci.itemid, ci.quantity, COALESCE(pci.quantity, 999999), ci.retailprice, ci.saleprice, pci.groupid
									FROM		tmp_cartitem ci
													JOIN sab_masterdata.pm_item i
															ON i.itemid = ci.itemid
													JOIN transaction.sm_promotion_comboitem pci
														ON (pci.itemid = ci.itemid OR pci.itemid = i.maingroupid :: varchar)
														AND pci.promotionid = v_promotionid;
									
									SELECT 	COUNT(DISTINCT groupid) 
									INTO		v_groupidcount
									FROM 		transaction.sm_promotion_comboitem 
									WHERE 	promotionid = v_promotionid;
									raise notice 'v_groupidcount: %', COALESCE(v_groupidcount, 0);
									--raise notice 'laivanduy: %', COALESCE((SELECT COUNT(DISTINCT groupid) FROM tmp_comboitem WHERE salequantity >= comboquantity), 0);
									
-- 									OPEN REF
-- 									FOR SELECT  1 a, *
-- 											FROM    tmp_comboitem t;
-- 									RETURN REF;
							
									/*
									SELECT	MIN(DIV(salequantity, comboquantity))
									INTO	v_combonumber
									FROM	tmp_comboitem;
									
									SELECT 	SUM(saleprice * v_combonumber * comboquantity) - v_value * v_combonumber
									INTO		v_promotion_discountvalue
									FROM		tmp_comboitem;
									*/
									IF v_promotiontype IN ('COMBO', 'COMBO_PP') THEN
											LOOP 
													EXIT WHEN (SELECT COUNT(DISTINCT groupid) FROM tmp_comboitem WHERE salequantity >= comboquantity) < v_groupidcount; 
													
													CREATE TEMP TABLE tmpItem AS
													SELECT DISTINCT ON (groupid)
																 groupid, itemid, retailprice, saleprice, salequantity, comboquantity
													FROM   tmp_comboitem
													WHERE	 salequantity >= comboquantity
													ORDER  BY groupid, retailprice DESC;
													
			-- 										OPEN REF
			-- 										FOR SELECT  1 a, *
			-- 												FROM    tmpItem t;
			-- 										RETURN REF;
													raise notice 'v_value: %', COALESCE(v_value, 0);
													SELECT 	--SUM(retailprice * comboquantity) - v_value 
																	CASE 	WHEN v_promotiontype = 'COMBO' THEN SUM(retailprice * comboquantity) - v_value 
																				WHEN v_promotiontype = 'COMBO_PP' THEN (SUM((CASE WHEN v_applydefaultpromotion THEN saleprice ELSE retailprice END)* comboquantity) * v_value) / 100
																				ELSE 1000
																	END
													INTO		v_promotion_discountvalue_combocount
													FROM		tmpItem;
													
													/*
													OPEN REF
													FOR SELECT  *, saleprice * comboquantity, v_value
															FROM    tmpItem;
													RETURN REF;
													*/
			-- 										raise notice 'v_promotion_discountvalue_combocount: %', COALESCE(v_promotion_discountvalue_combocount, 0);
													
													v_promotion_discountvalue := COALESCE(v_promotion_discountvalue, 0) + v_promotion_discountvalue_combocount;
			-- 										raise notice 'v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue, 0);
													
													UPDATE	tmp_comboitem AS ci
													SET			salequantity = ci.salequantity - ci.comboquantity
													FROM		tmpItem i
													WHERE		i.groupid = ci.groupid
																	AND i.itemid = ci.itemid;
													--v_groupidcount := 1000;
													DROP TABLE tmpItem;
												END LOOP ; 
									ELSE--'COMBOLIST_PP'
-- 											raise notice 'chia du: %', COALESCE((SELECT SUM(comboquantity) FROM tmp_comboitem) % 2, 99);
											IF (SELECT SUM(salequantity) FROM tmp_comboitem) % 2 = 0 THEN
													SELECT 	(SUM(retailprice * salequantity) * v_value) / 100
													INTO		v_promotion_discountvalue_combocount
													FROM		tmp_comboitem;
											ELSE
													SELECT MIN(retailprice) 
													INTO	v_promotion_discountvalue_combocount
													FROM	tmp_comboitem;
													
													SELECT 	((SUM(retailprice * salequantity) - v_promotion_discountvalue_combocount) * v_value) / 100
													INTO		v_promotion_discountvalue_combocount
													FROM		tmp_comboitem;
											END IF;
											
											
											
-- 											OPEN REF
-- 											FOR SELECT  *, saleprice * comboquantity, v_value, v_promotion_discountvalue_combocount
-- 																	,(SELECT SUM(salequantity) FROM tmp_comboitem) % 2 "Chia dư"
-- 													FROM    tmp_comboitem;
-- 											RETURN REF;
											
											--raise notice 'v_promotion_discountvalue_combocount: %', COALESCE(v_promotion_discountvalue_combocount, 0);
											
											v_promotion_discountvalue := COALESCE(v_promotion_discountvalue, 0) + v_promotion_discountvalue_combocount;
									END IF;
									
									--v_promotiontype := v_promotiontype;
							ELSEIF v_promotiontype = 'PCI' THEN
									DECLARE
											v_giftquantity int := v_socountitems / (v_socountitems_promotion + 1);
											v_quantity int;
											v_retailprice int;
											v_tempquantity_cur CURSOR FOR
												SELECT	quantity, retailprice
												FROM		tmp_cartitem
												ORDER BY retailprice;
									BEGIN
											v_promotion_discountvalue := 0;
											OPEN v_tempquantity_cur;
											LOOP
													FETCH v_tempquantity_cur INTO v_quantity, v_retailprice;
													EXIT WHEN NOT FOUND;
													EXIT WHEN v_giftquantity < 1;
													
													v_quantity := LEAST(v_quantity, v_giftquantity);
													v_giftquantity := v_giftquantity - v_quantity;
													
													v_promotion_discountvalue := v_promotion_discountvalue + v_quantity * v_retailprice;
													
													--raise notice 'v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue, -1111);
													--raise notice 'v_giftquantity: %', COALESCE(v_giftquantity, -1111);
											END LOOP;
									END;
									raise notice 'v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue, -1111);
									IF v_applydefaultpromotion = FALSE AND v_promotion_discountvalue > 0 THEN
											IF (SELECT SUM(quantity * (retailprice - saleprice)) FROM tmp_cartitem) > v_promotion_discountvalue THEN
													v_promotion_discountvalue := 0;
											END IF;
									END IF;
									raise notice 'v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue, -1111);
									raise notice 'v_promotiontype: %', COALESCE(v_promotiontype, '');
							END IF;
							--v_sototalamount_excludedefaultsales := 10000;
							raise notice 'v_value 204: %', COALESCE(v_value, 0);
							v_promotion_discountvalue := CASE 
																							WHEN v_promotiontype = 'PV' THEN v_value
																																											+ CASE WHEN v_iscalculatevip = FALSE AND v_replacevip = false AND COALESCE(v_voucherapplytogethercateidamount, 0) = 0 AND v_applyvipamount > 0 THEN COALESCE((
																																																																								SELECT    (SUM(CASE WHEN i.promotionid = 0 OR COALESCE(p.isapplysamevip, TRUE) = TRUE THEN i.quantity * i.saleprice ELSE 0 END) /*- v_value*/) * v_percentdiscount / 100
																																																																								FROM      tmp_cartitem i
																																																																													JOIN sab_masterdata.pm_item item 
																																																																														ON item.itemid = i.itemid
																																																																													LEFT JOIN sab_masterdata.pm_promotion p
																																																																														ON p.promotionid = i.promotionid
																																																																								WHERE			(item.maingroupid IN (5050, 5051,5052,5055, 6050, 6051,6052,6055, 5054))-- OR i.itemid ILIKE '%5054HW%')--, 5054
																																																																											--AND i.itemid NOT IN ('5051HB0068', '5051HB0069', '5051HB0070', '5051SD0025', '5051BP0021')
																																																																						), 0) ELSE 0 END--v_applyvipamount
																																											- CASE WHEN COALESCE(v_applydefaultpromotion, FALSE) = FALSE THEN v_diffpromotionamount ELSE 0 END
																							WHEN v_promotiontype = 'PVPI' THEN (v_value * v_socountitems) + v_applyvipamount -- (v_value * v_socountitems) * v_percentdiscount / 100 --v_applyvipamount - (v_value * v_socountitems) * v_percentdiscount / 100
																							WHEN v_promotiontype = 'PP' THEN (v_value / 100 * (CASE 		WHEN v_applydefaultpromotion AND v_caculateoriginalpriceproduct = FALSE THEN v_root_sototalamount -- v_sototalamount 
																																																					WHEN v_applydefaultpromotion AND v_caculateoriginalpriceproduct = TRUE THEN v_sototalamount_excludedefaultsales
																																																					WHEN v_applydefaultpromotion = FALSE AND v_replacevip = false THEN v_sorootamount --v_sototalamount, v_root_sototalamount_excludedefaultsales
																																																					ELSE v_sototalamount_excludedefaultsales
																																																			END
																																																			- CASE WHEN v_applyotherpromotion AND v_replacevip = false THEN COALESCE(v_discountamount, 0) ELSE 0 END
																																																			- CASE WHEN v_iscalculatevip = FALSE AND v_replacevip = false AND v_aftervip THEN COALESCE(v_applyvipamount, 0) ELSE 0 END
																																																			- v_includevoucheramount
																																																)
																																								)
																																								+ CASE WHEN v_iscalculatevip = FALSE AND v_replacevip = false AND v_aftervip THEN COALESCE(v_applyvipamount, 0) ELSE 0 END--Trừ thêm VIP do được áp dụng cùng CTKM
																																								+ COALESCE(v_applyvipamount_extra, 0)
																																								-- Cộng thêm phần mà giảm % với tỷ lệ giảm sản phầm < tỷ lệ % giảm của CTKM
																																								-- + CASE WHEN v_applydefaultpromotion = false THEN COALESCE(v_promotionamount_extra, 0) ELSE 0 END
																							WHEN v_promotiontype = 'PCI' THEN v_promotion_discountvalue
																							WHEN v_promotiontype = 'BUYONEGETONEFREE' THEN v_buyonegetonefree
																							WHEN v_promotiontype IN ('COMBO', 'COMBO_PP', 'COMBOLIST_PP') THEN COALESCE(v_promotion_discountvalue, 0)
																							ELSE 0
																							END;
								--v_promotion_discountvalue := CASE WHEN v_promotion_discountvalue < v_applyvipamount THEN 0 ELSE v_promotion_discountvalue - COALESCE(v_diffpromotionamount, 0) END;
								raise notice 'v_promotion_discountvalue - v_promotion_discountvalue-9999: %', COALESCE(v_promotion_discountvalue :: varchar, 'null');
-- 								IF v_promotiontype = 'PP' AND v_applydefaultpromotion = FALSE AND v_replacevip = false AND COALESCE(v_storeid, 1000) = 1000 THEN
-- 										v_promotion_discountvalue = v_promotion_discountvalue - COALESCE((
-- 														SELECT  	SUM(i.quantity * (i.retailprice - i.saleprice))
-- 														FROM      tmp_cartitem i JOIN sab_masterdata.pm_item item ON item.itemid = i.itemid
-- 														WHERE			(item.maingroupid IN (5050, 5051,5052,5055,6050, 6051,6052,6055))-- OR i.itemid ILIKE '%5054HW%')--, 5054
-- 												), 0);										
-- 								END IF;
								raise notice 'v_aftervip: %', COALESCE(v_aftervip, null);
								IF v_aftervip = false AND v_promotiontype = 'PP' THEN
-- 										raise notice 'v_aftervip = FALSE - v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue :: varchar, 'null');
										v_promotion_discountvalue = v_promotion_discountvalue + COALESCE(CASE WHEN COALESCE(v_storeid, 1000) = 1000 THEN v_root_sototalamount ELSE v_sorootamount END - v_promotion_discountvalue, 0) * v_percentdiscount / 100;
-- 										raise notice 'v_aftervip = FALSE - v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue :: varchar, 'null');
-- 										raise notice 'v_aftervip = FALSE - v_percentdiscount: %', COALESCE(v_percentdiscount :: varchar, 'null');
								END IF;
								v_promotion_discountvalue := v_promotion_discountvalue + COALESCE(v_voucherapplytogethercateidamount, 0);
								
								IF v_voucheramount > v_promotion_discountvalue THEN
										v_promotion_discountvalue := 0;
								END IF;
											
								raise notice 'v_promotion_discountvalue - v_applydefaultpromotion: %', COALESCE(v_applydefaultpromotion :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_sototalamount: %', COALESCE(v_sototalamount :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_sototalamount_excludedefaultsales: %', COALESCE(v_sototalamount_excludedefaultsales :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_value: %', COALESCE(v_value :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_applyvipamount: %', COALESCE(v_applyvipamount :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_diffpromotionamount: %', COALESCE(v_diffpromotionamount :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_root_sototalamount_excludedefaultsales: %', COALESCE(v_root_sototalamount_excludedefaultsales :: varchar, 'null');
								raise notice 'v_promotion_discountvalue - v_sorootamount: %', COALESCE(v_sorootamount :: varchar, 'null');
-- 								IF (SELECT SUM(quantity * (retailprice - saleprice)) FROM tmp_cartitem) > v_promotion_discountvalue AND COALESCE(v_applydefaultpromotion, false) = false THEN
-- 										v_promotion_discountvalue := 0;
-- 								END IF;
											
								IF v_promotiontype = 'SECONDPV' THEN
										CREATE TEMP TABLE tmp_calitem(
												id serial
												,itemid         VARCHAR(50)
												,quantity       INT
												,saleprice			INT
												,retailprice		INT
										);
										INSERT INTO tmp_calitem (itemid, quantity, saleprice, retailprice)
										SELECT	itemid, SUM(quantity), saleprice, retailprice
										FROM		tmp_cartitem
										GROUP BY itemid, saleprice, retailprice
										ORDER BY retailprice DESC;
										
										--Cộng thêm sản phẩm giá sale cuối cùng
										IF (SELECT SUM(quantity) FROM tmp_cartitem) % 2 = 1 THEN
												SELECT 	retailprice - saleprice
												INTO  	v_minprice
												FROM 		tmp_cartitem 
												ORDER BY saleprice
												LIMIT 1;
												UPDATE tmp_calitem SET quantity = quantity - 1 WHERE id = (SELECT MAX(id) id FROM tmp_calitem);
												raise notice '(SELECT SUM(quantity) FROM tmp_cartitem) % 2 = 1', COALESCE(v_minprice, '0');
										END IF;
										
										
										SELECT	SUM((CASE WHEN quantity = 1 THEN 1 ELSE quantity / 2 END) * (retailprice - v_value))
										INTO		v_promotion_discountvalue
										FROM		tmp_calitem
										WHERE		(SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id) % 2 = 1
														OR(
																COALESCE((SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id), 0) % 2 = 0
																AND quantity > 1
														)
														;
														
										
										
										raise notice 'v_minprice: %', COALESCE(v_minprice, '0');
										raise notice 'v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue, '0');
										v_promotion_discountvalue := v_promotion_discountvalue + COALESCE(v_minprice, 0);
														
-- 										OPEN REF
-- 										FOR 
-- 										SELECT	*
-- 														, (CASE WHEN quantity = 1 THEN 1 ELSE quantity / 2 END) slduockm
-- 														, COALESCE((SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id), 0)
-- 														,COALESCE((SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id), 0) % 2 = 0 SODU
-- 														,quantity
-- 										FROM		tmp_calitem
-- 										WHERE		(SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id) % 2 = 1
-- 														OR(
-- 																COALESCE((SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id), 0) % 2 = 0
-- 																AND quantity > 1
-- 														)
-- 														;
-- 										RETURN REF;
								END IF;
								
								IF v_promotiontype = 'GIFTBYAMOUNTPV' THEN
										DROP TABLE IF EXISTS tmp_calitem_GIFTBYAMOUNTPV;
										CREATE TEMP TABLE tmp_calitem_GIFTBYAMOUNTPV(
												id serial
												,productid         VARCHAR(50)
												,itemid         VARCHAR(50)
												,quantity       INT
												,saleprice			INT
												,retailprice		INT
										);
										DROP TABLE IF EXISTS tmp_calitem_GIFTBYAMOUNTPV2;
										CREATE TEMP TABLE tmp_calitem_GIFTBYAMOUNTPV2(
												productid         VARCHAR(50)
												,itemid         VARCHAR(50)
												,quantity       INT
												,saleprice			INT
												,retailprice		INT
										);
										DROP TABLE IF EXISTS tmp_GIFTBYAMOUNTPV_Result;
										CREATE TEMP TABLE tmp_GIFTBYAMOUNTPV_Result(
												numberofgift int
												,discountvalue		INT
										);
										INSERT INTO tmp_calitem_GIFTBYAMOUNTPV2(productid, itemid, quantity, saleprice, retailprice)
										SELECT	t.productid, t.itemid, t.quantity, t.saleprice, t.retailprice
										FROM		tmp_cartitem t JOIN sab_masterdata.pm_item i on i.itemid = t.itemid
										WHERE		i.maingroupid IN (5050, 5051,5052,5055, 6050, 6051,6052,6055);
										v_cnt := 0;
										
										LOOP
												INSERT INTO tmp_calitem_GIFTBYAMOUNTPV (productid, itemid, quantity, saleprice, retailprice)
												SELECT	productid, itemid, 1 quantity, saleprice, retailprice
												FROM		tmp_calitem_GIFTBYAMOUNTPV2
												WHERE		quantity > 0
												;
												GET DIAGNOSTICS v_cnt = ROW_COUNT;
												EXIT WHEN v_cnt = 0;
												UPDATE tmp_calitem_GIFTBYAMOUNTPV2 SET quantity = quantity - 1;
										END LOOP;
										DECLARE 	v_totalquantity INT;
															v_i INT = 1;
															v_i_max INT = 0;
															v_join varchar = '';
															v_colnotin varchar = '';
															v_amountminvalue INT;
										BEGIN
												SELECT 	conditionvaluefrom :: INT
												INTO		v_amountminvalue
												FROM 		TRANSACTION.sm_promotion_condition
												WHERE		objectid = 'SOROOTAMOUNT' AND promotionid = v_promotionid;
												SELECT	SUM(quantity)
												INTO		v_totalquantity
												FROM		tmp_calitem_GIFTBYAMOUNTPV;
												v_i_max := LEAST(v_totalquantity, v_sototalamount / v_amountminvalue, 6);
												LOOP
														
														IF (v_i > 1) THEN
																
																v_join := v_join || '
																JOIN tmp_calitem_GIFTBYAMOUNTPV ts' || v_i || ' ON ts' || v_i || '.id NOT IN (' || v_colnotin || ') AND ts' || v_i || '.itemid NOT ILIKE ''60%''';
														END IF;
														v_colnotin := v_colnotin || CASE WHEN v_colnotin = '' THEN '' ELSE ', ' END || 'ts' || (v_i) || '.id';
														v_sql := '
														INSERT INTO tmp_GIFTBYAMOUNTPV_Result(numberofgift, discountvalue)
														SELECT	' || v_i || ', ' || REPLACE(replace(v_colnotin, ',', '+'), 'id', 'retailprice') || ' - ' || (v_value * v_i) || '
																		+ CASE WHEN EXISTS(SELECT 1 FROM tmp_calitem_GIFTBYAMOUNTPV WHERE id NOT IN (' || v_colnotin || ') HAVING (SUM(retailprice) * (100 - ' || v_percentdiscount || ') / 100 * 1.1 / 1.1) >= ' || (v_amountminvalue * v_i) || ') THEN
																				COALESCE((SELECT SUM(retailprice) * ' || v_percentdiscount || ' / 100 FROM tmp_calitem_GIFTBYAMOUNTPV WHERE id NOT IN (' || v_colnotin || ') HAVING SUM(retailprice) * (100 - ' || v_percentdiscount || ') / 100 >= ' || (v_amountminvalue * v_i) || '), 0)
																				ELSE 0 END
														FROM		tmp_calitem_GIFTBYAMOUNTPV ts1
																		' || v_join || '
														WHERE		ts1.itemid NOT ILIKE ''60%''
																		AND EXISTS(SELECT 1 FROM tmp_calitem_GIFTBYAMOUNTPV WHERE id NOT IN (' || v_colnotin || ') HAVING (SUM(retailprice) * 1.1 / 1.1) >= ' || (v_amountminvalue * v_i) || ')
														ORDER BY	' || REPLACE(replace(v_colnotin, ',', '+'), 'id', 'retailprice') || ' 
																			+ CASE WHEN EXISTS(SELECT 1 FROM tmp_calitem_GIFTBYAMOUNTPV WHERE id NOT IN (' || v_colnotin || ') HAVING (SUM(retailprice) * (100 - ' || v_percentdiscount || ') / 100 * 1.1 / 1.1) >= ' || (v_amountminvalue * v_i) || ') THEN
																				COALESCE((SELECT SUM(retailprice) * ' || v_percentdiscount || ' / 100 FROM tmp_calitem_GIFTBYAMOUNTPV WHERE id NOT IN (' || v_colnotin || ') HAVING SUM(retailprice) * (100 - ' || v_percentdiscount || ') / 100 >= ' || (v_amountminvalue * v_i) || '), 0)
																				ELSE 0 END
																			DESC
														LIMIT 1;
														;
														';
														raise notice 'v_sql GIFTBYAMOUNTPV: %', COALESCE(v_sql, 'null');
														EXECUTE v_sql;
														
														IF NOT EXISTS(SELECT 1 FROM tmp_GIFTBYAMOUNTPV_Result WHERE numberofgift = v_i) THEN
																v_i := v_i_max;
														END IF;
														
														v_i = v_i + 1;
														
														EXIT WHEN v_i > v_i_max;														
												END LOOP;
										
										END;
										
-- 										--Cộng thêm sản phẩm giá sale cuối cùng
-- 										IF (SELECT SUM(quantity) FROM tmp_cartitem) % 2 = 1 THEN
-- 												SELECT 	retailprice - saleprice
-- 												INTO  	v_minprice
-- 												FROM 		tmp_cartitem 
-- 												ORDER BY saleprice
-- 												LIMIT 1;
-- 												UPDATE tmp_calitem SET quantity = quantity - 1 WHERE id = (SELECT MAX(id) id FROM tmp_calitem);
-- 												raise notice '(SELECT SUM(quantity) FROM tmp_cartitem) % 2 = 1', COALESCE(v_minprice, '0');
-- 										END IF;
-- 												
-- 										SELECT	SUM((CASE WHEN quantity = 1 THEN 1 ELSE quantity / 2 END) * (retailprice - v_value))
-- 										INTO		v_promotion_discountvalue
-- 										FROM		tmp_calitem
-- 										WHERE		(SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id) % 2 = 1
-- 														OR(
-- 																COALESCE((SELECT SUM(quantity) FROM tmp_calitem sub WHERE sub.id < tmp_calitem.id), 0) % 2 = 0
-- 																AND quantity > 1
-- 														)
-- 														;
-- 														
-- 										
-- 										
-- 										raise notice 'v_minprice: %', COALESCE(v_minprice, '0');
-- 										raise notice 'v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue, '0');
-- 										v_promotion_discountvalue := v_promotion_discountvalue + COALESCE(v_minprice, 0);
										v_promotion_discountvalue := 10000;
										SELECT MAX(discountvalue)
										INTO	v_promotion_discountvalue
										FROM 	tmp_GIFTBYAMOUNTPV_Result;
														
-- 										OPEN REF
-- 										FOR 
-- 										SELECT	*
-- 										FROM		tmp_calitem_GIFTBYAMOUNTPV
-- 														;
-- 										RETURN REF;
								END IF;
								
						END;
						raise notice 'v_promotion_discountvalue: %', COALESCE(v_promotion_discountvalue, '0');
						raise notice 'v_sototalamount_excludedefaultsales: %', COALESCE(v_sototalamount_excludedefaultsales, '0');
						raise notice 'v_sototalamount: %', COALESCE(v_sototalamount, '0');
						raise notice 'v_applyvipamount: %', COALESCE(v_applyvipamount, '0');
						raise notice 'v_replacevip: %', v_replacevip;
						--raise notice 'v_applydefaultpromotion: %', COALESCE(v_applydefaultpromotion, 'NULL');
						--raise notice 'v_caculateoriginalpriceproduct: %', COALESCE(v_caculateoriginalpriceproduct, 'NULL');
						
						IF v_promotiontype NOT IN ('GIFT') THEN
								INSERT INTO tmp_promotionvalues(promotionid, promotionname, promotiondetailid, discountvalue, isvip, isapplybirthday, promotiontype, applydefaultpromotion, applyotherpromotion, utilityid, iscalculatevip)
								VALUES(v_promotionid, v_promotionname, v_promotiondetailid, v_promotion_discountvalue, v_isvip, v_applybirthday, v_promotiontype, v_applydefaultpromotion, v_applyotherpromotion, v_utilityid, v_iscalculatevip);
								--raise notice 'v_promotion_discountvalue new1: %', COALESCE(v_promotion_discountvalue, -1111);
						END IF;
						
						IF v_source IN('OFF', 'ON') THEN--Link view gift product--'OFF', 'ON'--Trước kia sử dụng Page danh sách quà tặng, bây giờ ko sử dụng nên điều kiện này luôn true
								
								INSERT INTO tmp_promotionvalues(promotionid, promotiondetailid, promotionname, discountvalue, isvip, isapplybirthday, iscalculatevip, promotiontype
											, itemid, itemname, productid, productname
											, nameid
											, maingroupid
											, quantity
											, usedamount
											, image, colorid, size, applydefaultpromotion, applyotherpromotion
											,stockquantity
											,saleprice
											,retailprice
											,utilityid)
								SELECT DISTINCT v_promotionid, pd.promotiondetailid
											, v_promotionname, 0 discountvalue, v_isvip, v_applybirthday, v_iscalculatevip, promotiontype
											, pd.objectid itemid, pro.productname/*item.itemname*/, pro.productid, pro.productname
											, item.nameid
											, item.maingroupid
											, 1 quantity
											, COALESCE(pd.usedamount, v_usedamount) usedamount
											, (string_to_array(ii.image, ';'))[2] image, pro.colorid, pro.size, v_applydefaultpromotion, v_applyotherpromotion
											,COALESCE(cis.quantity, 0) - COALESCE(lockol.lockquantity, 0) stockquantity
											--,COALESCE(cis.quantity, 0) stockquantity
											,pd.value saleprice
											,(
													CASE
															WHEN PR_SALEPRICE.APPLYDATE IS NOT NULL AND now() >= PR_SALEPRICE.APPLYDATE THEN PR_SALEPRICE.NEWSALEPRICE
															ELSE PR_SALEPRICE.SALEPRICE
													END
											) AS retailprice
											,v_utilityid
								FROM	"transaction".sm_promotion_detail pd
												JOIN sab_masterdata.pm_product pro
													ON pro.itemid = pd.objectid
												JOIN sab_masterdata.pm_item item
													ON pro.itemid = item.itemid
												LEFT JOIN sab_masterdata.pm_item_image ii
													ON ii.itemid = pro.itemid
													AND ii.colorid = pro.colorid
													AND ii.orderindex = 1
												JOIN sab_masterdata.PR_SALEPRICE
													ON PR_SALEPRICE.itemid = pd.objectid
													AND PR_SALEPRICE.priceareaid = 1
												LEFT JOIN "transaction".pm_currentinstock cis
													ON cis.productid = pro.productid
													AND cis.storeid = v_storeid
												LEFT JOIN transaction.pm_saleorder_get_lockquantitylastdays(pro.productid, v_storeid) lockol
													ON lockol.productid = pro.productid
								WHERE		pd.promotionid = v_promotionid
												AND pd.promotiontype = 'GIFT'
												AND pro.isdeleted = false
												AND objecttypeid = 'ITEM'
												AND (
														pd.objectid :: varchar IN (
																SELECT 	c.conditionvalueto 
																FROM 		"transaction".sm_promotion_condition c --tmp_cartitem
																				JOIN sab_masterdata.pm_item i ON c.conditionvaluefrom = i.maingroupid :: varchar
																				JOIN tmp_cartitem tmp on tmp.itemid = i.itemid
																WHERE 	c.promotionid = v_promotionid 
																				AND c.objecttypeid = 'SOPRODUCT' 
																				AND c.objectid = 'ACCESSORYFORMAINGROUP'
														)
														OR NOT EXISTS(
																SELECT 	c.conditionvalueto 
																FROM 		"transaction".sm_promotion_condition c --tmp_cartitem
																				JOIN sab_masterdata.pm_item i ON c.conditionvaluefrom = i.maingroupid :: varchar
																				JOIN tmp_cartitem tmp on tmp.itemid = i.itemid
																WHERE 	c.promotionid = v_promotionid 
																				AND c.objecttypeid = 'SOPRODUCT' 
																				AND c.objectid = 'ACCESSORYFORMAINGROUP'
														)
												)
								;
								IF EXISTS(SELECT 1 FROM "transaction".sm_promotion_condition c WHERE c.promotionid = v_promotionid AND c.objecttypeid = 'SOPRODUCT' AND c.objectid = 'ACCESSORYFORMAINGROUP') THEN
										UPDATE	tmp_promotionvalues
										SET			usedamount = t.usedamount
														,giftquantity = t.quantity
										FROM		(SELECT conditionvalueto itemid, conditionvaluefrom maingroupid FROM "transaction".sm_promotion_condition c WHERE c.promotionid = v_promotionid AND c.objecttypeid = 'SOPRODUCT' AND c.objectid = 'ACCESSORYFORMAINGROUP') cgift
														JOIN
														(
																SELECT	i.maingroupid, SUM(saleprice * quantity) / SUM(quantity) usedamount, SUM(quantity) quantity
																FROM		tmp_cartitem st
																				JOIN sab_masterdata.pm_item i
																						ON i.itemid = st.itemid
																GROUP BY	i.maingroupid
														) t ON t.maingroupid :: varchar = cgift.maingroupid
										WHERE		tmp_promotionvalues.promotionid = v_promotionid
														AND cgift.itemid = tmp_promotionvalues.itemid
										;
								ELSE
-- 										raise notice 'v_root_sototalamount - v_discountamount: %', COALESCE((v_root_sototalamount - v_discountamount), '0');
-- 										raise notice 'v_usedamount: %', COALESCE((v_usedamount), '0');
										UPDATE	tmp_promotionvalues
										SET			giftquantity = "ceil"((v_root_sototalamount - v_discountamount) :: INT/usedamount :: INT)
										;
								END IF;
						ELSE
								INSERT INTO tmp_promotionvalues(promotionid, promotiondetailid, promotionname, discountvalue, isvip, isapplybirthday, iscalculatevip
											,promotiontype, itemid, itemname, productid, productname
											,nameid
											,maingroupid
											,quantity, usedamount, image, colorid, size, applydefaultpromotion, applyotherpromotion
											,stockquantity
											,saleprice
											,retailprice
											,utilityid)
								SELECT DISTINCT v_promotionid, pd.promotiondetailid
											,v_promotionname, 0 discountvalue, v_isvip, v_applybirthday, v_iscalculatevip
											,promotiontype, pd.objectid itemid, item.itemname, NULL productid, NULL productname
											,item.nameid
											,item.maingroupid
											,1 quantity, COALESCE(pd.usedamount, v_usedamount)
											,(
													SELECT 	(string_to_array(ii.image, ';'))[2]
													FROM 		sab_masterdata.pm_item_image ii
													WHERE		ii.itemid = pro.itemid
																	AND ii.orderindex = 1
													Limit 1
											) image
											,NULL colorid, NULL size, v_applydefaultpromotion, v_applyotherpromotion
											,0 stockquantity
											,pd.value saleprice
											,(
													CASE
															WHEN PR_SALEPRICE.APPLYDATE IS NOT NULL AND now() >= PR_SALEPRICE.APPLYDATE THEN PR_SALEPRICE.NEWSALEPRICE
															ELSE PR_SALEPRICE.SALEPRICE
													END
											) AS  retailprice
											,v_utilityid
								FROM	"transaction".sm_promotion_detail pd
												JOIN sab_masterdata.pm_product pro
													ON pro.itemid = pd.objectid
												JOIN sab_masterdata.pm_item item
													ON pro.itemid = item.itemid
												JOIN sab_masterdata.PR_SALEPRICE
													ON PR_SALEPRICE.itemid = pd.objectid
													AND PR_SALEPRICE.priceareaid = 1
								WHERE		pd.promotionid = v_promotionid
												AND pd.promotiontype = 'GIFT'
												AND pro.isdeleted = false
												AND objecttypeid = 'ITEM'
								LIMIT CASE WHEN COALESCE(v_page, 'cart') = 'cart' THEN 0 ELSE 999999 END
								;
						END IF;
        END IF;
    END LOOP;


IF v_isutility THEN
		UPDATE	tmp_promotionvalues
		SET			remainquantity = CASE WHEN ut.utilitytypeid = 'SHIP' AND v_storeid <> 1000 THEN 0 ELSE cpd.remainquantity END
						,utilityquantity = cpd.quantity
						,utilityusedquantity = cpd.usedquantity
						,paymethod = u.paymethod
						,utilitytypeid = ut.utilitytypeid
						,utilitytypename = ut.utilitytypename
						,groupapply = ut.groupapply
		FROM		"transaction".pm_customerutilitypackage cp
						JOIN "transaction".pm_customerutilitypackagedetail cpd
								ON cpd.customerutilitypackageid = cp.customerutilitypackageid
						JOIN "transaction".sm_utility u
								ON u.utilityid = cpd.utilityid
						JOIN transaction.sm_utilitytype ut
								ON ut.utilitytypeid = u.utilitytypeid
		WHERE		tmp_promotionvalues.utilityid = cpd.utilityid
						AND cp.customerid = v_customerid
						AND NOW() :: date BETWEEN cp.datefrom AND cp.dateto
						AND cp.isactived = true
						AND COALESCE(cp.isdeleted, false) = false
						
						AND cpd.remainquantity > 0
						;
		/*
		OPEN REF
		FOR SELECT  v_customerid, tmp_promotionvalues.utilityid, cpd.utilityid,cpd.customerutilitypackageid, cp.customerid, *
		FROM		tmp_promotionvalues 
						JOIN "transaction".pm_customerutilitypackagedetail cpd
								on tmp_promotionvalues.utilityid = cpd.utilityid
						join "transaction".pm_customerutilitypackage cp 
								ON cpd.customerutilitypackageid = cp.customerutilitypackageid
						LEFT JOIN "transaction".sm_utility u
								ON u.utilityid = cpd.utilityid
		--WHERE		cp.customerid = v_customerid
		;
		RETURN REF;
		*/
-- 		OPEN REF
-- 		FOR SELECT * from tmp_promotionvalues;
-- 		RETURN REF;
		
		DELETE FROM tmp_promotionvalues WHERE COALESCE(remainquantity, 0) = 0;
END IF;

DELETE FROM tmp_promotionvalues WHERE promotiontype IN ('COMBO', 'COMBO_PP', 'COMBOLIST_PP') AND discountvalue = 0;
DELETE FROM tmp_promotionvalues WHERE promotiontype IN ('COMBO', 'COMBO_PP', 'COMBOLIST_PP') AND promotionid NOT IN (
		SELECT 	promotionid
		FROM		tmp_promotionvalues
		WHERE 	promotiontype IN ('COMBO', 'COMBO_PP', 'COMBOLIST_PP')
		ORDER BY	discountvalue DESC
		LIMIT 1
);
DELETE FROM tmp_promotionvalues 
WHERE 	promotiontype IN ('PP', 'BUYONEGETONEFREE', 'GIFTBYAMOUNTPV')
-- 				AND isvip = false 
-- 				AND applydefaultpromotion = false 
				AND COALESCE(discountvalue, 0) <= 0 --v_sorootamount - v_root_sototalamount
				;

DELETE FROM tmp_promotionvalues 
WHERE 	promotiontype IN ('PP', 'PV') AND discountvalue <= 0
				;
DELETE FROM tmp_promotionvalues 
WHERE 	promotiontype IN ('GIFT') AND stockquantity <= 0
				AND v_source = 'OFF'
				;
DELETE FROM tmp_promotionvalues 
WHERE 	promotiontype IN ('GIFT') AND productid IN ('5048QT0017NGRE00', '5048QT0017NYEL00') AND v_storeid = 1000;-- AND COALESCE(v_source, 'OFF') IN ('ON');
IF NOW() :: DATE = '2022-08-28' :: DATE AND (to_char(now(), 'hh:mm:ss') BETWEEN '12:30:00' AND '13:05:00' OR to_char(now(), 'hh:mm:ss') BETWEEN '18:30:00' AND '19:05:00' OR to_char(now(), 'hh:mm:ss') BETWEEN '20:30:00' AND '21:05:00') THEN
		DELETE FROM tmp_promotionvalues WHERE promotionname ilike '%CHÀO ĐÓN CỬA HÀNG THỨ 30 - Giảm thêm%';
END IF;
				
-- raise notice 'v_sorootamount: %', COALESCE(v_sorootamount, '0');
-- raise notice 'v_root_sototalamount: %', COALESCE(v_root_sototalamount, '0');
-- raise notice 'total: %', COALESCE(v_sorootamount - v_root_sototalamount, '0');

OPEN REF
FOR SELECT  promotionid, promotionname, promotiondetailid, GREATEST(discountvalue, 0) discountvalue
-- 						,CASE WHEN (promotiontype IN('PV') AND (v_root_sototalamount_excludedefaultsales * (100 - v_percentdiscount) / 100) < 999000) OR promotiontype = 'SECONDPV' OR promotionname ILIKE '%Takashimaya%' THEN false ELSe true END isvip--, CASE WHEN promotiontype = 'BUYONEGETONEFREE' OR now() :: date >= '06/25/2022' THEN false ELSE true END isvip---------------- false
						,isvip
						,false isapplybirthday
						,0 giftquantity
						,iscalculatevip
						--,COALESCE(iscalculatevip, true) iscalculatevip
						,promotiontype
						,itemid, itemname, productid, productname
						,nameid
						,maingroupid, usedamount usedamount111
						, colorid, size, quantity, ((COALESCE(usedamount, (v_sototalamount - COALESCE(v_discountamount, 0))) * CASE WHEN isvip = true AND promotiontype = 'GIFT' THEN (100.00 - v_percentdiscount)/100.00 ELSE 1.00 END) * 1.1 / 1.1) :: INT8 usedamount
						, CASE WHEN v_source = 'OFF' THEN null ELSE image END image
-- 						, false applydefaultpromotion--remove
						, true applydefaultpromotion--remove
						,COALESCE(stockquantity, 0) stockquantity
						,retailprice
						,saleprice
						,utilityid
						,applyotherpromotion
						,utilitytypeid
						,utilitytypename
						,remainquantity
						,utilityquantity
						,utilityusedquantity
						,groupapply
						,paymethod
						--,v_applyvipamount
    FROM    tmp_promotionvalues t
    /*WHERE   discountvalue > 0 
						OR promotiontype = 'GIFT'
						--OR promotiontype = 'SHIP'*/
		ORDER BY discountvalue DESC;
RETURN REF;
END$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100