ALLOCATE_USERNAME='Patchwork'
ALLOCATE_PASSWORD='password'
ALLOCATE_IDENTITY_URL='https://identitytest.allocate-cloud.co.uk/core/connect/token'
ALLOCATE_URL='https://api-partner.allocate-dev.co.uk/v1'
PATCHWORK_USER_EMAIL='benjamin.harry@patchwork.health'
PATCHWORK_ORG_CODE='RSFT'
ALLOCATE_TRUST_CODE='RSCH'

organisation = Organisation.find_by(abbreviation: PATCHWORK_ORG_CODE)


if ExternalOrganisation.find_by(trust_code: trust_code).nil?
    puts "CREATING: trust code - #{trust_code}"
    ExternalOrganisation.new({ organisation: organisation,
                               trust_code: ALLOCATE_TRUST_CODE,
                               username: ALLOCATE_USERNAME,
                               password: ALLOCATE_PASSWORD,
                               identity_url: ALLOCATE_IDENTITY_URL,
                               url: ALLOCATE_URL }).save!
end
user = User.find_by(email: PATCHWORK_USER_EMAIL)
HubUser::ChangeOrganisation.call(
    { id: user.id, organisation_id: organisation.id },
    'current_user' => user
  )


def create_external_grade(organisation, grade_id, external_name, external_type, external_id, user)
    grade = organisation.grades.find_by(id: grade_id)
    if grade.external_grades.find_by(external_id: external_id).nil?
        puts "CREATING: external grade - #{external_name}"
        params = { grade_id: grade.id, 
                   external_name: external_name, 
                   external_type: external_type, 
                   external_id: external_id }
        result = ExternalGrade::Create.call(params, 'current_user' => user)
        if result.success?
            puts "SUCCESS:  #{external_name}"
        else 
            puts "FAILED: #{external_name}, #{result}"
        end
    end 
end

def create_external_request_reason(organisation, reason, external_id, user)  
    reason_for_request = ReasonForRequest.find_by(organisation: organisation, reason: reason)

    if reason_for_request.nil? 
        puts "CREATE: request reason - #{reason}"
        response = ReasonForRequest::Create.call({ reason: reason }, 'current_user' => valid_admin)
        if response.success?
            puts "SUCCESS: #{reason}"
            reason_for_request = response['model']
        else
            puts "FAILED: #{reason}"
            return
        end
    end 

    external_request_reason = ExternalRequestReason.find_by(organisation: organisation, 
                                                            reason_for_request: reason_for_request)
    if external_request_reason.nil?
      puts "CREATING: external request reason - #{reason}"
      external_request_reason = ExternalRequestReason.new(organisation: organisation, 
                                                          reason_for_request: reason_for_request, 
                                                          external_id: external_id)
      external_request_reason.save!
    end
    puts "SUCCESS: external request reason id: #{external_request_reason.id}, external id: #{external_request_reason.external_id}"
end

def external_unit(org, site_name, department_name, name, external_id, user)  
    department = Department.find_by(organisation: org, name: department_name)
    site = Site.find_by(organisation: org, name: site_name)
  
    if ExternalUnit.find_by(department: department, site: site).nil?
        puts "CREATING: external unit with department: #{department.name}, site: #{site.name}"
        result = ExternalUnit::Create.call({
                                  name: name,
                                  organisation_id: org.id,
                                  site_id: site.id,
                                  department_id: department.id,
                                  external_id: external_id,
                                  cost_centre: cost_centre.code,
                                }, 'current_user' => user)
        if result.success?
            puts "SUCCESS: External unit id: #{external_unit.id}, external id: #{external_unit.external_id}"
        else 
            puts "FAILURE: #{name}"
    end
  end

# Are external types needed?

create_external_grade(organisation, 2075, "AS", "AS", 31932, user)
create_external_grade(organisation, 2012, "SHO (ST1-2)", "ST1", 31647, user)
create_external_grade(organisation, 2013, "ST3-4", "ST3", 31659, user)
create_external_grade(organisation, 2014, "ST5-8", "ST5", 31655, user)
create_external_grade(organisation, 2018, "ST4 -ICU/Ana/ED Only", "ST4", 31654, user)
create_external_grade(organisation, 2020, "Cons", "Cons", 31685, user)
create_external_grade(organisation, 1986, "FY1", "FY1", 31638, user)
create_external_grade(organisation, 1987, "FY2", "FY2", 31639, user)
create_external_grade(organisation, 2019, "SD", "SD", 31931, user)

create_external_request_reason(organisation, "Annual Leave", 90012, user)
create_external_request_reason(organisation, "Vacancy", 90018, user)
create_external_request_reason(organisation, "High Acuity", 90003, user)
create_external_request_reason(organisation, "Deep Clean", 90013, user)
create_external_request_reason(organisation, "Annual Leave", 90012, user)
create_external_request_reason(organisation, "Major Incident", 90010, user)
create_external_request_reason(organisation, "Parental Leave", 90005, user)
create_external_request_reason(organisation, "Using up staff hours", 90020, user)
create_external_request_reason(organisation, "Infection", 90019, user)
create_external_request_reason(organisation, "Sickness", 90004, user)
create_external_request_reason(organisation, "Other", 90009, user)
create_external_request_reason(organisation, "Unplanned Leave", 90023, user)
create_external_request_reason(organisation, "Study Days/Training", 90006, user)
create_external_request_reason(organisation, "Sickness", 90004, user)
create_external_request_reason(organisation, "Special Leave", 90011, user)
create_external_request_reason(organisation, "Sickness", 90004, user)
create_external_request_reason(organisation, "Patient Escort", 90008, user)
create_external_request_reason(organisation, "Escalation", 90025, user)
create_external_request_reason(organisation, "Additional Beds/Extra Clinic", 90001, user)
create_external_request_reason(organisation, "Additional Beds/Extra Clinic", 90001, user)
create_external_request_reason(organisation, "Jury Service", 90016, user)
create_external_request_reason(organisation, "Additional Beds/Extra Clinic", 90001, user)
create_external_request_reason(organisation, "Sickness", 90004, user)
create_external_request_reason(organisation, "Sickness", 90004, user)
create_external_request_reason(organisation, "Vacancy", 90018, user)
create_external_request_reason(organisation, "Study Days/Training", 90006, user)
create_external_request_reason(organisation, "Special Leave", 90011, user)
create_external_request_reason(organisation, "Sickness", 90004, user)
create_external_request_reason(organisation, "Pressures", 90030, user)
create_external_request_reason(organisation, "Unplanned Leave", 90023, user)
create_external_request_reason(organisation, "Parental Leave", 90005, user)
create_external_request_reason(organisation, "Vacancy", 90018, user)
create_external_request_reason(organisation, "Sickness", 90004, user)
create_external_request_reason(organisation, "Waiting List Initiative", 90007, user)

external_unit(organisation, "Royal Surrey County Hospital","Radiotherapy Medics","Radiotherapy Medics","11871", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Radiology","Medics Radiology","10802", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Trauma & Orthopaedics","Medics Trauma & Orthopaedics","10764", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Pathology","Medics Pathology","10814", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Gynae Oncology","Medics Gynae Oncology","10758", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Orthodontics","Medics Orthodontics","10761", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Immunology","Medics Immunology","10813", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Ophthalmology","Medics Ophthalmology","10762", user)
external_unit(organisation, "Royal Surrey County Hospital","Medics Nuclear Service","Medics Nuclear Service","10739", user)
external_unit(organisation, "Royal Surrey County Hospital","ENT Medics","ENT Medics","11681", user)
external_unit(organisation, "Royal Surrey County Hospital","Anaesthetics Medics","Anaesthetics Medics","11751", user)
external_unit(organisation, "Royal Surrey County Hospital","AEC Medics","A&E","11365", user)
external_unit(organisation, "Royal Surrey County Hospital","ICU Medics","ICU Medics","11671", user)
external_unit(organisation, "Royal Surrey County Hospital","Paediatrics Medics","Paediatrics Medics","11672", user)
external_unit(organisation, "Royal Surrey County Hospital","Breast Medics","Breast Medics","10755", user)
external_unit(organisation, "Royal Surrey County Hospital","General Surgery Medics","General Surgery medics","10757", user)
external_unit(organisation, "Royal Surrey County Hospital","Neurophysiology Medics","Neurophysiology Medics","11901", user)
external_unit(organisation, "Royal Surrey County Hospital","OMFS Medics","OMFS Medics","11981", user)
external_unit(organisation, "Royal Surrey County Hospital","Oncology Medics","Oncology Medics","11651", user)
external_unit(organisation, "Royal Surrey County Hospital","Guildford Medics","Guildford Medics","11701", user)
external_unit(organisation, "Royal Surrey County Hospital","EAU MG and Cons Medics","EAU MG and Cons - Medics","11832", user)
external_unit(organisation, "Royal Surrey County Hospital","O&G Medics","O&G Medics","11691", user)
external_unit(organisation, "Royal Surrey County Hospital","MIC Hub Medics","MIC Hub Medics","11881", user)
external_unit(organisation, "Royal Surrey County Hospital","Urology Medics","Urology Medics","10765", user)
external_unit(organisation, "Royal Surrey County Hospital","Surgery OC Rota Medics","Surgery OC Rota Medics","11411", user)
external_unit(organisation, "Royal Surrey County Hospital","Rheum Medics","Rheum Medics","11369", user)
external_unit(organisation, "Royal Surrey County Hospital","Acute Medicine - GIM Rota","Acute Med - GIM","11371", user)
external_unit(organisation, "Royal Surrey County Hospital","Frailty Medics","Acute Frailty Medics","12231", user)
external_unit(organisation, "Royal Surrey County Hospital","EAU Juniors Medics","EAU Juniors Medics","11361", user)
external_unit(organisation, "Royal Surrey County Hospital","Albury Medics (Resp)","Albury Medics (Resp)","11368", user)
external_unit(organisation, "Royal Surrey County Hospital","AEC Medics","AEC Medics","12241", user)
external_unit(organisation, "Royal Surrey County Hospital","Plastics - Medics","Plastics - Medics","12321", user)
external_unit(organisation, "Royal Surrey County Hospital","Elstead Medics (Ageing & Health)","Elstead Medics (Ageing & Health)","12421", user)
external_unit(organisation, "Royal Surrey County Hospital","Eashing Medics (Ageing & Health)","Eashing Medics (Ageing & Health)","12422", user)
external_unit(organisation, "Royal Surrey County Hospital","Hindhead Medics (Ageing & Health)","Hindhead Medics (Ageing & Health)","12423", user)
external_unit(organisation, "Royal Surrey County Hospital","Ageing & Health Consultants","Ageing & Health Consultants","12441", user)
external_unit(organisation, "Royal Surrey County Hospital","Orthogeris Medics","Orthogeris Medics","12451", user)
external_unit(organisation, "Royal Surrey County Hospital","Neurology Medics","Neurology Medics","11367", user)
external_unit(organisation, "Royal Surrey County Hospital","Wisley Medics (Stroke)","Wisley Medics (Stroke)","12452", user)