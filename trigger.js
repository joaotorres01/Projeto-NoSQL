exports = function(changeEvent) {
    const fullDocument = changeEvent.fullDocument;
    const operationType = changeEvent.operationType;
    const userName = 'STORE'; // Replace with the appropriate user name
    const id = fullDocument._id;
  
    if (operationType === 'insert') {
      archive_employee_data(null, fullDocument, 'INSERT', userName, id);
    } else if (operationType === 'update') {
      const updatedFields = changeEvent.updateDescription.updatedFields;
  
      archive_employee_data(fullDocument, updatedFields, 'UPDATE', userName, id);
    }
  };
  
  function archive_employee_data(oldDocument, newDocument, eventType, userName, doc_id) {
    const employeesCollection = context.services.get("Cluster0").db("Store").collection("Employees");
  
    if(eventType === 'INSERT'){
    const archivedData = {
      event_date: new Date(),
      event_type: eventType,
      user_name: userName,
      old_employee_id: oldDocument ? oldDocument.EMPLOYEE_ID : null,
      old_first_name: oldDocument ? oldDocument.FIRST_NAME : null,
      old_middle_name: oldDocument ? oldDocument.MIDDLE_NAME : null,
      old_last_name: oldDocument ? oldDocument.LAST_NAME : null,
      old_date_of_birth: oldDocument ? oldDocument.DATE_OF_BIRTH : null,
      old_department_name: oldDocument ? oldDocument.DEPARTMENT_NAME : null,
      old_hire_date: oldDocument ? oldDocument.HIRE_DATE : null,
      old_salary: oldDocument ? oldDocument.SALARY : null,
      old_phone_number: oldDocument ? oldDocument.PHONE_NUMBER : null,
      old_email: oldDocument ? oldDocument.EMAIL : null,
      old_ssn_number: oldDocument ? oldDocument.SSN_NUMBER : null,
      old_manager_id: oldDocument ? oldDocument.MANAGER_ID : null,
      new_employee_id: newDocument ? newDocument.EMPLOYEE_ID : null,
      new_first_name: newDocument ? newDocument.FIRST_NAME : null,
      new_middle_name: newDocument ? newDocument.MIDDLE_NAME : null,
      new_last_name: newDocument ? newDocument.LAST_NAME : null,
      new_date_of_birth: newDocument ? newDocument.DATE_OF_BIRTH : null,
      new_department_name: newDocument ? newDocument.DEPARTMENT_NAME : null,
      new_hire_date: newDocument ? newDocument.HIRE_DATE : null,
      new_salary: newDocument ? newDocument.SALARY : null,
      new_phone_number: newDocument ? newDocument.PHONE_NUMBER : null,
      new_email: newDocument ? newDocument.EMAIL : null,
      new_ssn_number: newDocument ? newDocument.SSN_NUMBER : null,
      new_manager_id: newDocument ? newDocument.MANAGER_ID : null
    };
    
    const updatedDocument = {
      ...newDocument,
      employee_archive: archivedData
    };
    
  
    employeesCollection.replaceOne(
      { _id: doc_id },
      updatedDocument
    );
      
    }else {
      const archivedData = {
      event_date: new Date(),
      event_type: eventType,
      user_name: userName,
      old_employee_id: oldDocument ? oldDocument.employee_archive.new_employee_id : null,
      old_first_name: oldDocument ? oldDocument.employee_archive.new_first_name : null,
      old_middle_name: oldDocument ? oldDocument.employee_archive.new_middle_name : null,
      old_last_name: oldDocument ? oldDocument.employee_archive.new_last_name : null,
      old_date_of_birth: oldDocument ? oldDocument.employee_archive.new_date_of_birth : null,
      old_department_name: oldDocument ? oldDocument.employee_archive.new_department_name : null,
      old_hire_date: oldDocument ? oldDocument.employee_archive.new_hire_date : null,
      old_salary: oldDocument ? oldDocument.employee_archive.new_salary : null,
      old_phone_number: oldDocument ? oldDocument.employee_archive.new_phone_number : null,
      old_email: oldDocument ? oldDocument.employee_archive.new_email : null,
      old_ssn_number: oldDocument ? oldDocument.employee_archive.new_ssn_number : null,
      old_manager_id: oldDocument ? oldDocument.employee_archive.new_manager_id : null,
      new_employee_id: oldDocument ? oldDocument.EMPLOYEE_ID : null,
      new_first_name: oldDocument ? oldDocument.FIRST_NAME : null,
      new_middle_name: oldDocument ? oldDocument.MIDDLE_NAME : null,
      new_last_name: oldDocument ? oldDocument.LAST_NAME : null,
      new_date_of_birth: oldDocument ? oldDocument.DATE_OF_BIRTH : null,
      new_department_name: oldDocument ? oldDocument.DEPARTMENT_NAME : null,
      new_hire_date: oldDocument ? oldDocument.HIRE_DATE : null,
      new_salary: oldDocument ? oldDocument.SALARY : null,
      new_phone_number: oldDocument ? oldDocument.PHONE_NUMBER : null,
      new_email: oldDocument ? oldDocument.EMAIL : null,
      new_ssn_number: oldDocument ? oldDocument.SSN_NUMBER : null,
      new_manager_id: oldDocument ? oldDocument.MANAGER_ID : null
    };
    
    const updatedDocument = {
      ...oldDocument,
      employee_archive: archivedData
    };
    
  
    employeesCollection.replaceOne(
      { _id: doc_id },
      updatedDocument
    );
    }
  
  }
  