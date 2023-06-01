exports = function(changeEvent) {
    const employeesArchive = context.services.get("Cluster0").db("Store").collection("Employees_Archive");
  
    const event_type = changeEvent.operationType;
  
    let archive_entry = {
      EVENT_DATE: new Date(),
      EVENT_TYPE: event_type,
      USER_NAME: 'STORE',
      OLD_EMPLOYEE_ID: null,
      OLD_FIRST_NAME: null,
      OLD_MIDDLE_NAME: null,
      OLD_LAST_NAME: null,
      OLD_DATE_OF_BIRTH: null,
      OLD_DEPARTMENT_ID: null,
      OLD_MANAGER_ID: null,
      OLD_HIRE_DATE: null,
      OLD_SALARY: null,
      OLD_PHONE_NUMBER: null,
      OLD_EMAIL: null,
      OLD_SSN_NUMBER: null,
      NEW_EMPLOYEE_ID: null,
      NEW_FIRST_NAME: null,
      NEW_MIDDLE_NAME: null,
      NEW_LAST_NAME: null,
      NEW_DATE_OF_BIRTH: null,
      NEW_DEPARTMENT_ID: null,
      NEW_MANAGER_ID: null,
      NEW_HIRE_DATE: null,
      NEW_SALARY: null,
      NEW_PHONE_NUMBER: null,
      NEW_EMAIL: null,
      NEW_SSN_NUMBER: null
    };
  
    if (event_type === "insert") {
      archive_entry.NEW_EMPLOYEE_ID = changeEvent.fullDocument.employee_id;
      archive_entry.NEW_FIRST_NAME = changeEvent.fullDocument.first_name;
      archive_entry.NEW_MIDDLE_NAME = changeEvent.fullDocument.middle_name;
      archive_entry.NEW_LAST_NAME = changeEvent.fullDocument.last_name;
      archive_entry.NEW_DATE_OF_BIRTH = changeEvent.fullDocument.date_of_birth;
      archive_entry.NEW_DEPARTMENT_ID = changeEvent.fullDocument.department ? changeEvent.fullDocument.department.department_id : null;
      archive_entry.NEW_MANAGER_ID = changeEvent.fullDocument.manager_id;
      archive_entry.NEW_HIRE_DATE = changeEvent.fullDocument.hire_date;
      archive_entry.NEW_SALARY = changeEvent.fullDocument.salary;
      archive_entry.NEW_PHONE_NUMBER = changeEvent.fullDocument.phone_number;
      archive_entry.NEW_EMAIL = changeEvent.fullDocument.email;
      archive_entry.NEW_SSN_NUMBER = changeEvent.fullDocument.ssn_number;
    } else if (event_type === "update") {
      
      archive_entry.OLD_EMPLOYEE_ID = changeEvent.fullDocumentBeforeChange.employee_id;
      archive_entry.OLD_FIRST_NAME = changeEvent.fullDocumentBeforeChange.first_name;
      archive_entry.OLD_MIDDLE_NAME = changeEvent.fullDocumentBeforeChange.middle_name;
      archive_entry.OLD_LAST_NAME = changeEvent.fullDocumentBeforeChange.last_name;
      archive_entry.OLD_DATE_OF_BIRTH = changeEvent.fullDocumentBeforeChange.date_of_birth;
      archive_entry.OLD_DEPARTMENT_ID = changeEvent.fullDocumentBeforeChange.department ? changeEvent.fullDocumentBeforeChange.department.department_id : null;
      archive_entry.OLD_MANAGER_ID = changeEvent.fullDocumentBeforeChange.manager_id;
      archive_entry.OLD_HIRE_DATE = changeEvent.fullDocumentBeforeChange.hire_date;
      archive_entry.OLD_SALARY = changeEvent.fullDocumentBeforeChange.salary;
      archive_entry.OLD_PHONE_NUMBER = changeEvent.fullDocumentBeforeChange.phone_number;
      archive_entry.OLD_EMAIL = changeEvent.fullDocumentBeforeChange.email;
      archive_entry.OLD_SSN_NUMBER = changeEvent.fullDocumentBeforeChange.ssn_number;
  
  
      const new_employee = changeEvent.fullDocument;
  
      archive_entry.NEW_EMPLOYEE_ID = new_employee ? new_employee.employee_id : null;
      archive_entry.NEW_FIRST_NAME = new_employee ? new_employee.first_name : null;
      archive_entry.NEW_MIDDLE_NAME = new_employee ? new_employee.middle_name : null;
      archive_entry.NEW_LAST_NAME = new_employee ? new_employee.last_name : null;
      archive_entry.NEW_DATE_OF_BIRTH = new_employee ? new_employee.date_of_birth : null;
      archive_entry.NEW_DEPARTMENT_ID = new_employee && new_employee.department ? new_employee.department.department_id : null;
      archive_entry.NEW_MANAGER_ID = new_employee ? new_employee.manager_id : null;
      archive_entry.NEW_HIRE_DATE = new_employee ? new_employee.hire_date : null;
      archive_entry.NEW_SALARY = new_employee ? new_employee.salary : null;
      archive_entry.NEW_PHONE_NUMBER = new_employee ? new_employee.phone_number : null;
      archive_entry.NEW_EMAIL = new_employee ? new_employee.email : null;
      archive_entry.NEW_SSN_NUMBER = new_employee ? new_employee.ssn_number : null;
    } else if (event_type === "delete") {
      const old_employee = changeEvent.fullDocumentBeforeChange;
  
      archive_entry.OLD_EMPLOYEE_ID = old_employee ? old_employee.employee_id : null;
      archive_entry.OLD_FIRST_NAME = old_employee ? old_employee.first_name : null;
      archive_entry.OLD_MIDDLE_NAME = old_employee ? old_employee.middle_name : null;
      archive_entry.OLD_LAST_NAME = old_employee ? old_employee.last_name : null;
      archive_entry.OLD_DATE_OF_BIRTH = old_employee ? old_employee.date_of_birth : null;
      archive_entry.OLD_DEPARTMENT_ID = old_employee && old_employee.department ? old_employee.department.department_id : null;
      archive_entry.OLD_MANAGER_ID = old_employee ? old_employee.manager_id : null;
      archive_entry.OLD_HIRE_DATE = old_employee ? old_employee.hire_date : null;
      archive_entry.OLD_SALARY = old_employee ? old_employee.salary : null;
      archive_entry.OLD_PHONE_NUMBER = old_employee ? old_employee.phone_number : null;
      archive_entry.OLD_EMAIL = old_employee ? old_employee.email : null;
      archive_entry.OLD_SSN_NUMBER = old_employee ? old_employee.ssn_number : null;
    }
  
    employeesArchive.insertOne(archive_entry);
  };
  