using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml;

public class reglamentFFOMS
{
  
    public static bool handling_file(string file, List<clsConnections> link_connections, string[] folders, ReglamentLinker reglamentLinker, out string result_comments)
    {
        result_comments = "";
        bool result = false;
        try
        {
 
                switch (reglamentLinker.link.prefixFile)
                {
                    case "F003":
                        result = processing_f003(ref link_connections, ref folders, out result_comments, file);    
                        break;
                    default: //unknown messageType
                        break;
                }
                if (!result)
                    throw new Exception("...");
                else
                    result_comments += " - handled";
          
        }
        catch (Exception ss)
        {
            result_comments = " error! in metod " + result_comments;
        }
        return result;
    }

    public static bool processing_f003(ref List<clsConnections> link_connections, ref string[] folders, out string comments, string file = "")
    {
        comments = String.Empty;
        bool result = false;
        //----------------
        try
        {
            string version;

            XmlDocument xmlDocument = new XmlDocument();
            xmlDocument.Load(file);

            version = xmlDocument.SelectSingleNode("packet").Attributes.GetNamedItem("version").Value;
            ReglamentLinker reglamentLinker = new ReglamentLinker();
            reglamentLinker.getLink(null, null, version, Path.GetFileName(file));

            switch (version)
            {
                case "1.0.1":
                    result = processing_f003_1_0_1(ref link_connections, out comments, ref xmlDocument);
                    break;
            }
        }
        catch (Exception e)
        {
            comments += e.Message;
        }

        return result;
    }

    public static bool processing_f003_1_0_1(ref List<clsConnections> link_connections, out string comments, ref XmlDocument xmlDocument)
    {
        comments = String.Empty;
        bool result = false;
        //----------------
        Schemes_FFOMS.f003_schema_1_0_1.packet packet = new Schemes_FFOMS.f003_schema_1_0_1.packet();
        try
        {
            packet = clsLibrary.DeserializeFrom<Schemes_FFOMS.f003_schema_1_0_1.packet>(xmlDocument.DocumentElement);
            // Получаем метаданные 
            Schemes_FFOMS.f003_schema_1_0_1.medCompany[] medCompanys = packet.medCompany;
            //создаем временную таблицу
            result = clsLibrary.execQuery_PGR(ref link_connections, "postgres",
                "DROP TABLE IF EXISTS buf_eir.tmpf003; " +
                "CREATE TABLE buf_eir.tmpf003(" +
                "tf_okato varchar NULL, mcod varchar(6) NULL, nam_mop varchar NULL, nam_mok varchar NULL, inn varchar(100) NULL, ogrn varchar(100) NULL, kpp varchar(100) NULL," +
                "juraddress xml NULL, okopf int4 NULL, vedpri int4 NULL, org int4 NULL, fam_ruk varchar(100) NULL, im_ruk varchar(100) NULL, ot_ruk varchar(100) NULL, " +
                "phone varchar(100) NULL, fax varchar(100) NULL, e_mail varchar(100) NULL, docs xml NULL, www varchar NULL, medinclude xml NULL, medadvices xml NULL, d_edit date NULL);");
            List<string[]> list = new List<string[]>();            
            foreach (Schemes_FFOMS.f003_schema_1_0_1.medCompany medCompany in medCompanys)
            {
                string[] row = new string[22];
                row[0] = medCompany.tf_okato;
                row[1] = medCompany.mcod;
                row[2] = XmlHelper.SerializeClear<Schemes_FFOMS.f003_schema_1_0_1.jurAddress>(medCompany.jurAddress);
                row[3] = medCompany.nam_mop;
                row[4] = medCompany.nam_mok;
                row[5] = medCompany.inn;
                row[6] = medCompany.Ogrn;
                row[7] = medCompany.KPP;
                row[8] = medCompany.okopf;
                row[9] = medCompany.vedpri;
                row[10] = medCompany.org;
                row[11] = medCompany.fam_ruk;
                row[12] = medCompany.im_ruk;
                row[13] = medCompany.ot_ruk;
                row[14] = medCompany.phone;
                row[15] = medCompany.fax;
                row[16] = medCompany.e_mail;
                row[17] = XmlHelper.SerializeClear<Schemes_FFOMS.f003_schema_1_0_1.doc[]>(medCompany.doc);
                row[18] = medCompany.www;
                row[19] = XmlHelper.SerializeClear<Schemes_FFOMS.f003_schema_1_0_1.medInclude>(medCompany.medInclude); 
                row[20] = XmlHelper.SerializeClear<Schemes_FFOMS.f003_schema_1_0_1.medAdvice[]>(medCompany.medAdvice);
                row[21] = medCompany.d_edit.Substring(6, 4) + "-" + medCompany.d_edit.Substring(3, 2) + "-" + medCompany.d_edit.Substring(0, 2);
                list.Add(row);
            }
            result = clsLibrary.execQuery_PGR_insertList(ref link_connections, "postgres",
                "insert into buf_eir.tmpf003 (tf_okato, mcod, juraddress, nam_mop, nam_mok, inn, ogrn, kpp, okopf, vedpri, org, fam_ruk, im_ruk, ot_ruk, phone, fax, e_mail, docs, www, medinclude, medadvices, d_edit) values ",
                ref list, 100);
            result = clsLibrary.execQuery_PGR(ref link_connections, "postgres",
                "with list as ( select src.* from buf_eir.tmpf003 src left outer join library.f003 lib on lib.mcod = src.mcod where lib.mcod is null ) " +
                "insert into library.f003 (tf_okato, mcod, juraddress, nam_mop, nam_mok, inn, ogrn, kpp, okopf, vedpri, org, fam_ruk, im_ruk, ot_ruk, phone, fax, e_mail, docs, www, medinclude, medadvices, d_edit) " +
                "select tf_okato, mcod, juraddress, nam_mop, nam_mok, inn, ogrn, kpp, okopf, vedpri, org, fam_ruk, im_ruk, ot_ruk, phone, fax, e_mail, docs, www, medinclude, medadvices, d_edit from list; " +

                "with list as ( select src.*from buf_eir.tmpf003 src left outer join library.f003 lib on lib.mcod = src.mcod where lib.d_edit < src.d_edit ) " +
                "update library.f003 set " +
                "tf_okato = l.tf_okato, juraddress = l.juraddress, nam_mop = l.nam_mop, nam_mok = l.nam_mok, inn = l.inn, ogrn = l.ogrn, kpp = l.kpp, okopf = l.okopf, vedpri = l.vedpri, org = l.org, " +
                "fam_ruk = l.fam_ruk, im_ruk = l.im_ruk, ot_ruk = l.ot_ruk, phone = l.phone, fax = l.fax, " +
                "e_mail = l.e_mail, docs = l.docs, www = l.www, medinclude = l.medinclude, medadvices = l.medadvices, d_edit = l.d_edit from list l where library.f003.mcod = l.mcod; " +

                "DROP TABLE IF EXISTS buf_eir.tmpf003; "
                );    
        }
        catch (Exception e)
        {
            comments += e.Message;
        } 
        return result;
    }

}