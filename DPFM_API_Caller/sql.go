package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-brand-doc-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-brand-doc-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var headerDoc *[]dpfm_api_output_formatter.HeaderDoc

	for _, fn := range accepter {
		switch fn {
		case "HeaderDoc":
			func() {
				headerDoc = c.HeaderDoc(input, output, errs, log)
			}()
		}
	}

	data := &dpfm_api_output_formatter.Message{
		HeaderDoc: headerDoc,
	}

	return data
}

func (c *DPFMAPICaller) HeaderDoc(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.HeaderDoc {
	where := "WHERE 1 = 1"

	if input.HeaderDoc.Brand != nil && *input.HeaderDoc.Brand != 0 {
		where = fmt.Sprintf("%s\nAND Brand = %d", where, *input.HeaderDoc.Brand)
	}
	if input.HeaderDoc.DocType != nil && len(*input.HeaderDoc.DocType) != 0 {
		where = fmt.Sprintf("%s\nAND DocType = '%v'", where, *input.HeaderDoc.DocType)
	}
	if input.HeaderDoc.DocIssuerBusinessPartner != nil && *input.HeaderDoc.DocIssuerBusinessPartner != 0 {
		where = fmt.Sprintf("%s\nAND DocIssuerBusinessPartner = %v", where, *input.HeaderDoc.DocIssuerBusinessPartner)
	}
	groupBy := "\nGROUP BY Brand, DocType, DocIssuerBusinessPartner "

	rows, err := c.db.Query(
		`SELECT
    Brand, DocType, MAX(DocVersionID), DocID, FileExtension, FileName, FilePath, DocIssuerBusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_brand_header_doc_data
		` + where + groupBy + `;`)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeaderDoc(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
