using Capstone.ManagedReceiving.DAL.Enums;
using Capstone.ManagedReceiving.DAL.Helpers;
using Capstone.ManagedReceiving.DAL.Interfaces;
using Capstone.ManagedReceiving.DAL.Models;
using Capstone.ManagedReceiving.DAL.Services;
using Capstone.ManagedReceiving.DAL.Validators;
using Itenso.TimePeriod;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Capstone.ManagedReceiving.DAL.Extensions;
using Z.EntityFramework.Plus;
using static Microsoft.ApplicationInsights.MetricDimensionNames.TelemetryContext;
using System.Web.Http;
using System.Web.Http.ModelBinding;
using System.Windows.Input;

namespace Capstone.ManagedReceiving.DAL
{
    public class AppointmentHelper : IAppointmentHelper
    {
        private readonly MRContext db;
        private readonly IAppointmentProvider appointmentProvider;
        private readonly IGetDockCapacitiesService _dockCapacitiesService;

        public AppointmentHelper(MRContext db)
        {
            this.db = db;
            _dockCapacitiesService = new GetDockCapacitiesService(db);
            this.appointmentProvider = new AppointmentProvider();
        }

        public AppointmentHelper(MRContext db, IAppointmentProvider appointmentProvider)
        {
            this.db = db;
            this.appointmentProvider = appointmentProvider;
            _dockCapacitiesService = new GetDockCapacitiesService(db);
        }

        public AppointmentHelper(MRContext db, IAppointmentProvider appointmentProvider, IGetDockCapacitiesService dockCapacitiesService)
        {
            this.db = db;
            this.appointmentProvider = appointmentProvider;
            _dockCapacitiesService = dockCapacitiesService;
        }


        public bool IsReservationValidForAppt(Reservation reservation, Appointment appointment)
        {
            if (reservation.SiteID != appointment.SiteID)
                return false;

            if (reservation.Carriers.Any())
            {
                if (reservation.Carriers.All(c => appointment.CarrierID != c.ID))
                    return false;
            }

            var vendorIDs = new List<int>();

            if (reservation.Vendors.Any())
            {
                if (appointment.Orders != null)
                {
                    vendorIDs.AddRange(appointment.Orders.Select(o => o.VendorID).ToList());

                    vendorIDs.AddRange(appointment.Orders.SelectMany(o => o.OrderDetails).Select(od => od.VendorID).ToList());

                    return reservation.Vendors.Any(v => vendorIDs.Contains(v.ID));


                }
                else
                {
                    return false;
                }

            }





            return true;
        }


        public bool IsAppointmentPastScheduleChangeCutoff(Appointment appointment)
        {
            if (appointment.Site == null)
                db.Entry(appointment).Reference(a => a.Site).Load();
            if (appointment.Doors == null)
                db.Entry(appointment).Collection(a => a.Doors).Load();
            if (appointment.Orders == null)
                db.Entry(appointment).Collection(a => a.Orders).Load();

            var vendorIDs = appointment.Orders.Select(o => o.VendorID).Distinct().ToList();
            if (db.Vendors.Where(v => vendorIDs.Contains(v.ID)).All(v => v.AllowSameDayAppointment))
                return false;

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(appointment.Site.TimeZone);

            foreach (var door in appointment.Doors)
            {
                db.Entry(door).Reference(d => d.Dock).Load();
                if (DateTime.TryParse(door.Dock.ScheduleCutoffTime, out DateTime cutoffTime))
                {
                    cutoffTime = TimeZoneInfo.ConvertTimeToUtc(cutoffTime, siteTimeZone);

                    var siteAppointmentCalendarDate = TimeZoneInfo.ConvertTimeFromUtc(appointment.StartTime.AddHours(appointment.Site.BusinessDayOffset), siteTimeZone).Date;
                    var siteTomorrow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, siteTimeZone).Date.AddDays(1);

                    if (DateTime.UtcNow > cutoffTime && siteAppointmentCalendarDate <= siteTomorrow)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public async Task<bool> IsAppointmentPastScheduleChangeCutoffAsync(Appointment appointment, CancellationToken cancellationToken)
        {
            if (appointment.Site == null)
            {
                await db.Entry(appointment).Reference(a => a.Site).LoadAsync(cancellationToken);
            }
            if (appointment.Doors == null)
            {
                await db.Entry(appointment).Collection(a => a.Doors).LoadAsync(cancellationToken);
            }

            if (AllVendorsAllowSameDayAppointment(appointment, cancellationToken).Result) 
                return false;

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(appointment.Site.TimeZone);

            foreach (Door door in appointment.Doors)
            {
                await db.Entry(door).Reference(d => d.Dock).LoadAsync(cancellationToken);

                if (DateTime.TryParse(door.Dock.ScheduleCutoffTime, out DateTime cutoffTime))
                {
                    cutoffTime = TimeZoneInfo.ConvertTimeToUtc(cutoffTime, siteTimeZone);

                    var siteAppointmentCalendarDate = TimeZoneInfo.ConvertTimeFromUtc(appointment.StartTime.AddHours(appointment.Site.BusinessDayOffset), siteTimeZone).Date;

                    var siteTomorrow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, siteTimeZone).Date.AddDays(1);

                    if (DateTime.UtcNow > cutoffTime && siteAppointmentCalendarDate <= siteTomorrow)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public async Task<bool> AllVendorsAllowSameDayAppointment(Appointment appointment, CancellationToken cancellationToken)
        {
            if (appointment.Orders == null)
            {
                await db.Entry(appointment).Collection(a => a.Orders).LoadAsync(cancellationToken);
            }

            var vendorIDs = appointment.Orders.Select(o => o.VendorID).Distinct().ToList();

            if (db.Vendors.Where(v => vendorIDs.Contains(v.ID)).All(v => v.AllowSameDayAppointment))
            {
                return true;
            }

            return false;
        }


        public TimeRange GetValidDateRangeForDock(Dock dock, List<DateTime> dueDates, TimeZoneInfo siteTimeZone)
        {
            //What to do if the early/late has no value? All dates are allowed.

            //Will need to be midnight in UTC for the site

            var periods = new TimePeriodCollection();

            foreach (var d in dueDates)

            {
                var startDate = DateTime.MinValue;

                if ((d - DateTime.MinValue).TotalDays > dock.EarlyScheduleThreshold)
                {
                    startDate = FixUTCMidnight(siteTimeZone, d.AddDays(-dock.EarlyScheduleThreshold));
                }


                var endDate = DateTime.MaxValue;

                if ((DateTime.MaxValue - d).TotalDays > dock.LateScheduleThreshold)
                {
                    endDate = FixUTCMidnight(siteTimeZone, d.AddDays(dock.LateScheduleThreshold + 1)).AddSeconds(-1);
                }
                periods.Add(new TimeRange(startDate, endDate));
            }





            if (periods.Count == 1)
                return (TimeRange)periods.First();

            var periodIntersector =
                    new TimePeriodIntersector<TimeRange>();

            ITimePeriodCollection appointmentDateRange = new TimePeriodCollection();



            for (int i = 0; i < periods.Count; i++)
            {
                if (!appointmentDateRange.IntersectsWith(periods[i]))
                    return null;

                appointmentDateRange = periodIntersector.IntersectPeriods(new TimePeriodCollection
                {
                    appointmentDateRange,
                    periods[i]
                });


            }

            //var appointmentDateRange = periodIntersector.IntersectPeriods(periods);

            if (appointmentDateRange.Count == 1)
                return (TimeRange)appointmentDateRange.First();


            return null;
        }


        public ResultSingleRecord<DoorGroupAndDocks> GetDoorGroup(int siteId, IEnumerable<SlotOrder> orders,
            int doorGroupId = 0, List<Vendor> vendors = null, int carrierId = 0, int deliveryCarrierId = 0)
        {
            var result = new ResultSingleRecord<DoorGroupAndDocks>
            {
                Messages = new List<Message>(),
                Data = new DoorGroupAndDocks(),
                Success = false
            };

            Site site = db.Sites.FirstOrDefault(s => s.ID == siteId);

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(site.TimeZone);

            // carrier of record should always be passed in (never null) - making it optional to avoid breaking change
            // return result message if carrierId isn't passed in here
            var doorGroupIdHold = doorGroupId; //site carrier override door group override trumps this setting
            bool usingCarrierDoorGroupOverride = false;

            //todo - this may need to be added in later if the UI isn't ready yet
            //if (carrierId == 0)
            //{
            //    result.Messages.Add(new Message
            //        { Code = "PARAMETER_REQUIRED", Text = "Carrier of Record must be passed in to check for possible Carrier Door Group Override" });
            //    return result;
            //}

            int carrierOfRecordOverrideDoorGroupId = 0;
            int deliveryCarrierOverrideDoorGroupId = 0;
            if (carrierId != 0 || deliveryCarrierId != 0)
            {
                var doorGroupIds = db.SiteCarriers.Where(sc =>
                    (sc.CarrierID == deliveryCarrierId || sc.CarrierID == carrierId) && sc.SiteID == siteId).AsNoFilter();
                foreach (var siteCarrier in doorGroupIds)
                {
                    if (siteCarrier.CarrierID == carrierId)
                        carrierOfRecordOverrideDoorGroupId = siteCarrier.DoorGroupID ?? 0;
                    else
                        deliveryCarrierOverrideDoorGroupId = siteCarrier.DoorGroupID ?? 0;
                }
            }

            if (carrierOfRecordOverrideDoorGroupId != 0 || deliveryCarrierOverrideDoorGroupId != 0)
                usingCarrierDoorGroupOverride = true;

            doorGroupId = carrierOfRecordOverrideDoorGroupId != 0
                ? carrierOfRecordOverrideDoorGroupId
                : (deliveryCarrierOverrideDoorGroupId != 0 ? deliveryCarrierOverrideDoorGroupId : doorGroupIdHold);

            //todo - do we skip all the logic involving orders and order details if usingCarrierDoorGroupOverride == true, or do we leave it in the flow
            //end

            List<int> rackIDs = orders.Where(o => o.OrderDetails != null).SelectMany(o => o.OrderDetails)
                .Select(d => d.RackID).Distinct().ToList();

            var racks = db.Racks.Where(r => r.SiteID == siteId && rackIDs.Contains(r.ID)).ToList();

            if (vendors == null)
            {
                var vendorIDs = orders.Select(o => o.VendorID).ToList();
                vendorIDs.AddRange(orders.Where(o => o.OrderDetails != null).SelectMany(o => o.OrderDetails).Select(od => od.VendorID).ToList());

                vendorIDs = vendorIDs.Distinct().ToList();

                vendors = db.Vendors.Where(v => v.SiteID == siteId && vendorIDs.Contains(v.ID)).ToList();
            }

            var doorGroupQuantities = new[]
            {
                new{id = 0, quantity = 1.1}

            }.ToList();

            doorGroupQuantities.RemoveAll(x => x.id == 0);

            foreach (var o in orders.Where(o => o.OrderDetails != null).ToList())
            {
                var details = o.OrderDetails
                    .Where(od =>
                        racks.Any(r => r.ID == od.RackID) ||
                        vendors.FirstOrDefault(v => v.ID == od.VendorID).DoorGroupID.HasValue)
                    .Select(od =>
                    {
                        return new
                        {
                            od.CaseCount,
                            od.PalletHI,
                            od.PalletTI,
                            DoorGroupID = vendors.FirstOrDefault(v => v.ID == od.VendorID).DoorGroupID ??
                                          racks.First(r => r.ID == od.RackID).DoorGroupID
                        };
                    })
                    .ToList();

                if (details.Any(d =>
                        d.CaseCount.GetValueOrDefault() == 0 || d.PalletHI.GetValueOrDefault() == 0 ||
                        d.PalletTI.GetValueOrDefault() == 0))
                {
                    var orderDoorGroup =
                        details.GroupBy(d => d.DoorGroupID).Select(g => new
                        {
                            dgID = g.Key,
                            Count = g.Count()
                        }
                        ).OrderByDescending(g => g.Count).FirstOrDefault();

                    doorGroupQuantities.Add(new
                    {
                        id = orderDoorGroup.dgID,
                        quantity = site.UnitType == UnitTypeEnum.Pallets ? (double)o.PalletCount : (double)o.CaseCount
                    });
                }
                else
                {
                    doorGroupQuantities.AddRange(
                        details.GroupBy(d => d.DoorGroupID).Select(g => new
                        {
                            id = g.Key,
                            quantity = g.Sum(c =>
                                site.UnitType == UnitTypeEnum.Pallets
                                    ? c.CaseCount.GetValueOrDefault() /
                                      (c.PalletHI.GetValueOrDefault() * c.PalletTI.GetValueOrDefault())
                                    : c.CaseCount.GetValueOrDefault()
                            )
                        }).ToList()
                    );
                }
            }

            var resultsMessage = string.Empty;

            DoorGroup closestDoorGroup;

            //todo - is the whole first part of this if statement redundant since it is being performaed again at line 340?
            if (!doorGroupId.Equals(0))
            {
                var id = doorGroupId;
                closestDoorGroup = db.DoorGroups.Where(dg => dg.ID.Equals(id) && dg.SiteID.Equals(siteId))
                    .Include(dg => dg.Doors).Include(dg => dg.Doors.Select(d => d.Dock)).FirstOrDefault();
                if (closestDoorGroup == null)
                {
                    result.Messages.Add(new Message
                    { Code = "INVALID_DOORGROUP", Text = "Requested door group not found." });
                    return result;
                }
            }
            else
            {
                if (doorGroupQuantities.Count > 0)
                    doorGroupId = doorGroupQuantities.GroupBy(q => q.id)
                        .Select(dg => new { id = dg.Key, quantity = dg.Sum(q => q.quantity) })
                        .OrderByDescending(d => d.quantity).FirstOrDefault().id;
            }

            if (doorGroupId.Equals(0)) //No door group could be determined from the orders
            {
                resultsMessage = "Closest Door Group (from site): ";

                if (!site.DefaultDoorGroup.HasValue || site.DefaultDoorGroup == 0)
                {
                    result.Messages.Add(new Message { Code = "NO_DDG", Text = "Site has no default door group" });
                    return result;
                }

                doorGroupId = site.DefaultDoorGroup.Value;
            }

            closestDoorGroup = db.DoorGroups.Where(dg => dg.SiteID == siteId && dg.ID.Equals(doorGroupId) && dg.SiteID.Equals(siteId))
                .Include(dg => dg.Doors).Include(dg => dg.Doors.Select(d => d.Dock)).FirstOrDefault();

            if (closestDoorGroup == null)
            {
                result.Messages.Add(new Message { Code = "INVALID_DOORGROUP", Text = resultsMessage + " not found" });
                return result;
            }

            result.Data.DoorGroupName = closestDoorGroup.Name;
            result.Data.DoorGroupID = closestDoorGroup.ID;

            var docks = closestDoorGroup.Doors.Where(d => d.Active).Select(d => d.Dock).Distinct().ToList();

            if (!usingCarrierDoorGroupOverride && !site.AllowApptOrdersDiffDock)
            {
                if ((!GetCommonDocks(docks, doorGroupQuantities.Select(od => od.id).Distinct().ToList()).Any()) || orders.Any(o => o.OrderDetails == null))
                {
                    result.Messages.Add(new Message { Code = "SAME_DOCK_RESTRICTION", Text = string.Format("No docks found in common for all orders.") });
                }
            }

            foreach (var dock in docks)
            {
                var validDateRange = GetValidDateRangeForDock(dock,
                    orders.Select(o => o.DueDate.Value).ToList(),
                    siteTimeZone
                );

                result.Data.DockList = result.Data.DockList ?? new List<DockItem>();
                var di = new DockItem()
                {
                    DockName = dock.Name,
                    DockID = dock.ID
                };
                if (validDateRange == null)
                {
                    result.Messages.Add(new Message
                    {
                        Code = "DOCK_DATE_THRESHOLD",
                        Text = "No appointment date will accommodate the orders based on their due dates and " +
                               $"dock scheduling thresholds for dock '{dock.Name}'."
                    });
                }
                else
                {
                    di.FirstDate = TimeZoneInfo.ConvertTimeFromUtc(validDateRange.Start, siteTimeZone);
                    di.LastDate = TimeZoneInfo.ConvertTimeFromUtc(validDateRange.End, siteTimeZone);
                }
                result.Data.DockList.Add(di);
            }

            result.Success = false;

            if (result.Data.DockList != null)
            {
                var docksWithValidDateRanges =
                    result.Data.DockList.Where(d => d.FirstDate.HasValue && d.LastDate.HasValue).ToList();

                if (docksWithValidDateRanges.Count > 0)
                {
                    result.Data.DeliveryWindowExists = true;

                    result.Data.FirstDate = docksWithValidDateRanges.Select(d => d.FirstDate).Min();
                    result.Data.LastDate = docksWithValidDateRanges.Select(d => d.LastDate).Max();

                    //Adjust ranges to null for UI
                    if (result.Data.FirstDate.Value.Year < 1000)
                        result.Data.FirstDate = null;
                    if (result.Data.LastDate.Value.Year > 3000)
                        result.Data.LastDate = null;

                    result.Data.IdealDate = DateTime.SpecifyKind(
                        orders.Where(o => o.DueDate.HasValue)
                            .OrderByDescending(
                                o => site.UnitType.Equals(UnitTypeEnum.Pallets) ? o.PalletCount : o.CaseCount)
                            .FirstOrDefault().DueDate.Value, DateTimeKind.Utc);
                }
            }
            return result;
        }

        private int getPeriods(int siteId, int incrementMinutes)
        {
            string openTime = "00:00";
            string closeTime = "23:59";

            int openMinuteOfDay = Convert.ToInt32(MRUtils.getMillisFromTime(openTime) / (60 * 1000));
            int offset = Convert.ToInt32(openMinuteOfDay);

            int closeMinuteOfDay = Convert.ToInt32(MRUtils.getMillisFromTime(closeTime) / (60 * 1000));

            openMinuteOfDay -= offset;
            closeMinuteOfDay -= offset;
            if (closeMinuteOfDay < 0)
            {
                closeMinuteOfDay += 24 * 60;
            }
            if (closeMinuteOfDay == 1439)
            {
                closeMinuteOfDay++;
            }
            int minutesInDay = closeMinuteOfDay - openMinuteOfDay;
            int periods = minutesInDay / incrementMinutes;
            return periods;
        }

        #region MyRegion

        //private List<string> getClientSlots(Site site, DateTime appointDate, double appointmentDateMillis, int incrementMinutes, int appointmentDuration)
        //{
        //    int siteId = site.ID;
        //    string openTime = "00:00";
        //    string closeTime = "23:59";
        //    double firstMillisCheck = 0;
        //    double lastMillisCheck = 0;
        //    string clientTimezone = site.TimeZone;
        //    //TODO: Check for open time. When do slots start without open time at site?


        //    if (site.BusinessDayOffset < 0)
        //    {
        //        openTime = MRUtils.getTimeFromDateTime(DateTime.Parse(openTime).AddHours(site.BusinessDayOffset).ToString());

        //        // DateTime appointmentDate = MRUtils.getDateFromMillis(appointmentDateMillis);
        //        //for the last slot check
        //        DateTime openDateBusinessOffsetCheck = (appointDate.AddDays(-1).Add(TimeSpan.Parse(openTime)));
        //        DateTime closeDateBusinessOffsetCheck = (appointDate.Add(TimeSpan.Parse(closeTime)));
        //        double openDateBusinessOffsetCheckInMillis = MRUtils.getDateTimeInMillis(openDateBusinessOffsetCheck);
        //        double closeDateBusinessOffsetCheckInMillis = MRUtils.getDateTimeInMillis(closeDateBusinessOffsetCheck);

        //        firstMillisCheck = openDateBusinessOffsetCheckInMillis;
        //        lastMillisCheck = closeDateBusinessOffsetCheckInMillis;
        //    }
        //    else
        //    {
        //        firstMillisCheck = MRUtils.getMillisFromTime(openTime);
        //        lastMillisCheck = MRUtils.getMillisFromTime(closeTime);
        //    }

        //    //get total periods
        //    int periods = getPeriods(siteId, incrementMinutes);

        //    //double earliestMillis = (MRUtils.getTimeInMillis(DateTime.Now) - (30 * 60 * 1000));
        //    double earliestMillis = (MRUtils.getOnlyTimeInMillis(DateTime.Now) + (30 * 60 * 1000));

        //    double firstMillis = MRUtils.getMillisFromTime(openTime);
        //    double lastMillis = MRUtils.getMillisFromTime(closeTime);

        //    bool isToday = false;
        //    if (MRUtils.getDateFromMillis(appointmentDateMillis).ToString("MM/dd/yyyy") == DateTime.Now.ToString("MM/dd/yyyy"))
        //    {
        //        isToday = true;
        //    }

        //    List<string> slotTimes = new List<string>();
        //    double incrementMillis = incrementMinutes * 60 * 1000;

        //    for (int i = 0; i < periods; ++i)
        //    {
        //        double beginMillis = firstMillis + i * incrementMillis;
        //        //to check if there would be enough time in the last slot
        //        double endMillisCheck = firstMillisCheck + (i * incrementMillis) + (appointmentDuration * 60 * 1000);

        //        if (endMillisCheck < lastMillisCheck)
        //        {
        //            if (isToday)
        //            {
        //                if (beginMillis >= earliestMillis)
        //                {

        //                    slotTimes.Add(MRUtils.getTimeHHMMAAAFromMillis(beginMillis));
        //                }
        //            }
        //            else
        //            {
        //                slotTimes.Add(MRUtils.getTimeHHMMAAAFromMillis(beginMillis));
        //            }
        //        }
        //    }
        //    return slotTimes;
        //}

        #endregion

        public List<DateTime> getClientSlotsUTC(Site site, DateTime appointDate, double appointmentDateMillis, int incrementMinutes, int appointmentDuration)
        {

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(site.TimeZone);

            if (appointDate.Kind != DateTimeKind.Utc)
                appointDate = TimeZoneInfo.ConvertTimeToUtc(appointDate, siteTimeZone);



            var slotTimes = new List<DateTime>();

            var maxSlot = appointDate.AddDays(1);

            maxSlot = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, maxSlot, new TimeSpan(site.BusinessDayOffset, 0, 0), false);

            var newSlot = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointDate, new TimeSpan(site.BusinessDayOffset, 0, 0), false);



            if (appointmentDuration > 1440)
                appointmentDuration = 1440;

            while (newSlot <= maxSlot.AddMinutes(-appointmentDuration))
            {
                slotTimes.Add(newSlot);
                newSlot = newSlot.AddMinutes(incrementMinutes);
            }

            return slotTimes;
        }


        private bool isEquipmentAvailableForSlot(int siteId, DateTime st, int appointmentDuration, List<Appointment> appointments, List<Vendor> appointmentVendors, List<Vendor> allVendors, List<Equipment> equipment, List<Door> allDoors)
        {
            // return true;
            bool result = true;
            int equipDoorGrpTotal = 0;
            int equipDockTotal = 0;


            foreach (var vendor in appointmentVendors.Where(v => v.EquipmentGroupID.HasValue))
            {

                var allVendorsAppointWithEquipGroup = from v in allVendors
                                                      join a in appointments on v.ID equals a.VendorID

                                                      where v.EquipmentGroupID == vendor.EquipmentGroupID &&
                                                            TimeExtensions.DoDateRangesOverlap(
                                                          a.GateInTime ?? a.StartTime, (a.GateInTime ?? a.StartTime).AddMinutes(a.ScheduledDuration),
                                                          st, st.AddMinutes(appointmentDuration)
                                                       )
                                                      select new
                                                      {
                                                          vendorAppointmentDoorId = a.Doors.Select(d => d.ID)
                                                      };


                var equipSite = equipment.Where(e => e.EquipmentTypeID == vendor.EquipmentGroupID && e.Availability == EquipmentAvailabilityEnum.Site).FirstOrDefault();
                var equipDGrp = equipment.Where(e => e.EquipmentTypeID == vendor.EquipmentGroupID && e.Availability == EquipmentAvailabilityEnum.DoorGroup).FirstOrDefault();
                var equipDocks = equipment.Where(e => e.EquipmentTypeID == vendor.EquipmentGroupID && e.Availability == EquipmentAvailabilityEnum.Dock).FirstOrDefault();

                //HashSet allows only the unique values to the list
                HashSet<int> appointDoorGrpIds = new HashSet<int>();
                HashSet<int> appointDockIds = new HashSet<int>();

                foreach (var appointment in allVendorsAppointWithEquipGroup)
                {

                    var door = allDoors.Where(d => appointment.vendorAppointmentDoorId.Contains(d.ID)).FirstOrDefault();
                    int doorGroupId = door.DoorGroupID;
                    int dockId = door.DockID;

                    appointDoorGrpIds.Add(doorGroupId);
                    appointDockIds.Add(dockId);
                }


                if (equipSite != null && allVendorsAppointWithEquipGroup.Count() >= equipSite.Quantity)
                {
                    result = false;
                    break;
                }
                else if ((equipDGrp != null && appointDoorGrpIds.Count > 0) || (equipDocks != null && appointDockIds.Count > 0))
                {

                    foreach (var doorGroupId in appointDoorGrpIds)
                    {
                        var equipDoorGrp = equipment.Where(e => e.EquipmentTypeID == vendor.EquipmentGroupID && e.Availability == EquipmentAvailabilityEnum.DoorGroup && e.SiteID == siteId).Select(s => s.DoorGroups.Where(w => w.ID == doorGroupId)).AsQueryable().Count();

                        equipDoorGrpTotal += equipDoorGrp;
                    }
                    foreach (var dockId in appointDockIds)
                    {
                        var equipDock = equipment.Where(e => e.EquipmentTypeID == vendor.EquipmentGroupID && e.Availability == EquipmentAvailabilityEnum.Dock && e.SiteID == siteId).Select(s => s.Docks.Where(w => w.ID == dockId)).AsQueryable().Count();

                        equipDockTotal += equipDock;
                    }

                    if (equipDGrp != null && (equipDoorGrpTotal >= equipDGrp.Quantity))
                    {
                        result = false;
                        break;
                    }
                    if (equipDocks != null && (equipDockTotal >= equipDocks.Quantity))
                    {
                        result = false;
                        break;
                    }
                }

            }
            return result;
        }

        private double? CalculateAppointmentDuration(Appointment appointment, Site site)
        {
            DateTime? StartTime = null;
            switch (site.CustomMPUCalcStart)
            {
                case Enums.AppointmentStartOptionsEnum.AppointmentTime:
                    StartTime = appointment.StartTime;
                    break;
                case Enums.AppointmentStartOptionsEnum.GatedIn:
                    StartTime = appointment.GateInTime;
                    break;
                case Enums.AppointmentStartOptionsEnum.OnComplex:
                    StartTime = appointment.OnComplexTime;
                    break;
                case Enums.AppointmentStartOptionsEnum.UnloadStart:
                    StartTime = appointment.MMSUnloadStartTime ?? appointment.GateInTime;
                    break;

                default:
                    break;

            }

            DateTime? EndTime = null;
            switch (site.CustomMPUCalcEnd)
            {
                case Enums.AppointmentEndOptionsEnum.GatedOut:
                    EndTime = appointment.GateOutTime;
                    break;
                case Enums.AppointmentEndOptionsEnum.UnloadEnd:
                    EndTime = appointment.MMSUnloadEndTime ?? appointment.GateOutTime;
                    break;
                case Enums.AppointmentEndOptionsEnum.OffComplex:
                    EndTime = appointment.OffComplexTime;
                    break;
                default:
                    break;

            }
            if (EndTime.HasValue && StartTime.HasValue)
                return (EndTime.Value - StartTime.Value).TotalMinutes + site.CustomMPUCalcBuffer;
            return null;
        }

        public void RecalculateVendorUnloadTimes(Appointment appointment)
        {
            if (!appointment.AppointmentPalletOverride.HasValue)
            {

                //var appointment = db.Appointments.Include(a => a.Orders).Include(a => a.Orders.Select(o => o.Vendor)).Where(a => a.ID == appointmentID).FirstOrDefault();

                var site = db.Sites.FirstOrDefault(s => s.ID == appointment.SiteID);

                var vendorIDs = appointment.Orders.Select(o => o.VendorID).Distinct().ToList();

                var vendors = db.Vendors.Where(v => vendorIDs.Contains(v.ID)).ToList();

                db.SkipHistory = true;

                foreach (var vendor in vendors)
                {

                    //If there's not at least 4 weeks of history for this vendor,
                    //do not calculate average unload time per unit
                    var historyStartDate = DateTime.UtcNow.AddDays(-28);
                    int vendorID = vendor.ID;
                    if (!db.Appointments.Include(a => a.Orders)
                        .Where(a =>
                        a.Orders.Any(o => o.VendorID.Equals(vendor.ID)) &&
                        a.GateOutTime.HasValue &&
                        a.AppointmentDate <= historyStartDate
                        ).Any())
                        continue;

                    //The site has a setting for how many weeks back to look to compute
                    //average unload time per unit, if it's not present, use 2 weeks
                    var wksAvgUnloadTime = site.wksAvgUnloadTime ?? 2;
                    var earliestAppointmentDate = DateTime.UtcNow.AddDays(-7 * (double)wksAvgUnloadTime);

                    //To calculate the average, we need all the other gated in/out appointments for this vendor
                    //over that time period
                    var durationList = db.Appointments.Include(a => a.Orders)
                   .Where(a =>
                   a.AppointmentDate >= earliestAppointmentDate &&
                   a.GateOutTime.HasValue && a.GateInTime.HasValue &&
                   a.Orders.Any(o => o.VendorID.Equals(vendor.ID)) &&
                   a.ID != appointment.ID
                   )
                   .ToList();

                    var totalUnits = durationList.Select(a => OrdersService.GetOrderTotalUnitsCountNoOverride(site, a)).Sum();

                    double totalTime;
                    if (vendor.MinutesPerUnit.HasValue)
                        totalTime = totalUnits * vendor.MinutesPerUnit.Value;
                    else
                    {
                        totalTime = durationList.Select(a => (a.GateOutTime.Value - a.GateInTime.Value).TotalMinutes).Sum();
                    }

                    if (appointment.GateOutTime.HasValue && appointment.GateInTime.HasValue)
                    {
                        totalUnits += OrdersService.GetOrderTotalUnitsCountNoOverride(site, appointment);

                        totalTime += (appointment.GateOutTime.Value - appointment.GateInTime.Value).TotalMinutes;

                    }

                    var avgMinutesPerUnit = totalTime / totalUnits;

                    if (vendor.MaxCalcMinutesPerUnit.HasValue)
                    {
                        if (avgMinutesPerUnit > vendor.MaxCalcMinutesPerUnit.Value)
                            avgMinutesPerUnit = vendor.MaxCalcMinutesPerUnit.Value;
                    }
                    else if (site.MaxCalcMinutesPerUnit.HasValue)
                    {
                        if (avgMinutesPerUnit > site.MaxCalcMinutesPerUnit.Value)
                            avgMinutesPerUnit = site.MaxCalcMinutesPerUnit.Value;
                    }

                    vendor.MinutesPerUnit = avgMinutesPerUnit;

                    db.SaveChanges();

                    if (!(site.CustomMPUCalcStart.HasValue && site.CustomMPUCalcEnd.HasValue))
                        continue;

                    var totalCustomUnits = durationList.Where(a => CalculateAppointmentDuration(a, site).HasValue).Select(a => OrdersService.GetOrderTotalUnitsCountNoOverride(site, a)).Sum();

                    if (CalculateAppointmentDuration(appointment, site).HasValue)
                        totalCustomUnits += OrdersService.GetOrderTotalUnitsCountNoOverride(site, appointment);

                    if (totalCustomUnits == 0)
                        continue;

                    double totalCustomTime;

                    if (vendor.CustomMinutesPerUnit.HasValue)
                        totalCustomTime = totalUnits * vendor.CustomMinutesPerUnit.Value;
                    else
                    {

                        totalCustomTime = durationList
                            .Where(a => CalculateAppointmentDuration(a, site).HasValue)
                            .Select(a => CalculateAppointmentDuration(a, site).Value).Sum();

                    }

                    totalCustomTime += CalculateAppointmentDuration(appointment, site) ?? 0;

                    var avgCustomMinutesPerUnit = totalCustomTime / totalCustomUnits;

                    if (vendor.MaxCalcCustomMinutesPerUnit.HasValue)
                    {
                        if (avgCustomMinutesPerUnit > vendor.MaxCalcCustomMinutesPerUnit.Value)
                            avgCustomMinutesPerUnit = vendor.MaxCalcCustomMinutesPerUnit.Value;
                    }
                    else
                    if (site.MaxCalcMinutesPerUnit.HasValue)
                    {
                        if (avgCustomMinutesPerUnit > site.MaxCalcMinutesPerUnit.Value)
                            avgCustomMinutesPerUnit = site.MaxCalcMinutesPerUnit.Value;
                    }

                    vendor.CustomMinutesPerUnit = avgCustomMinutesPerUnit;

                    db.SaveChanges();
                }
                db.SkipHistory = false;
            }
        }


        public IQueryable<Appointment> GetAppointments(int siteId, DateTime requestedDate)
        {
            var site = db.Sites.Include(s => s.Docks).Include(s => s.DoorGroups).Where(s => s.ID == siteId).FirstOrDefault();

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(site.TimeZone);

            var businessDate = TimeZoneInfo.ConvertTimeToUtc(requestedDate, siteTimeZone).AddHours(site.BusinessDayOffset);

            var businessEndDate = businessDate.AddDays(1);


            return appointmentProvider.GetAppointments(db, businessDate, businessEndDate, siteId).AsQueryable();
        }

        public IEnumerable<ScheduledAppointment> GetScheduledAppointments(int siteId, DateTime StartDate, DateTime EndDate)
        {
            return appointmentProvider.GetScheduledAppointments(db, StartDate, EndDate, siteId);
        }

        public List<AppointmentDockDaily> GetSiteDockCapacities(int siteId, DateTime requestDate)
        {
            var site = db.Sites.Include(s => s.Docks)
                .Include(s => s.Docks.Select(d => d.Doors))
                .Include(s => s.DoorGroups).FirstOrDefault(s => s.ID == siteId);

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(site.TimeZone);

            DateTime requestDateUtc = TimeZoneInfo.ConvertTimeToUtc(requestDate, siteTimeZone);

            DateTime businessStartDateUtc = requestDateUtc.AddHours(site.BusinessDayOffset);

            DateTime businessEndDateUtc = businessStartDateUtc.AddDays(1);

            List<Reservation> reservations = db.Reservations.AsNoFilter()
                .Where(c =>
                (c.Active) &&
                (c.EffectiveStartDate == null || c.EffectiveEndDate >= businessStartDateUtc &&
                (c.EffectiveEndDate == null || c.EffectiveStartDate <= businessEndDateUtc)) &&
                (c.SiteID == siteId)
                )
              .Include(r => r.Doors)
              .ToList();

            DateTime localBusinessStartDate = TimeZoneInfo.ConvertTimeFromUtc(businessStartDateUtc, siteTimeZone);

            string localDayOfWeek = localBusinessStartDate.DayOfWeek.GetHashCode().ToString();

            var reservedSlots = reservations.Where(r =>
                   (r.EffectiveStartDate == null || r.EffectiveStartDate.Value <= businessStartDateUtc) &&
                   (r.EffectiveEndDate == null || r.EffectiveEndDate.Value >= businessStartDateUtc) &&
                   (r.DayOfWeek.Contains(localDayOfWeek)) &&
                   (!r.Exceptions.Contains(localBusinessStartDate.Date)) &&
                   (
                   (site.BusinessDayOffset >= 0 && r.StartTime.Hours >= site.BusinessDayOffset) ||
                   (site.BusinessDayOffset < 0 && r.StartTime.Hours <= 24 + site.BusinessDayOffset)
                   )
                   )
                .Select(a => ReservedSlotMapper.MapFromReservation(a, siteTimeZone, businessStartDateUtc)).ToList();

            int dayShift = 0;

            if (site.BusinessDayOffset > 0)
            {
                dayShift = 1;
            }
            else if (site.BusinessDayOffset < 0)
            {
                dayShift = -1;
            }

            if (dayShift != 0)
            {
                DateTime shiftedLocalBusinessStartDate = localBusinessStartDate.AddDays(dayShift);

                int shiftedLocalDayOfWeek = shiftedLocalBusinessStartDate.DayOfWeek.GetHashCode();

                DateTime shiftedBusinessStartDateUtc = businessStartDateUtc.AddDays(dayShift);

                reservedSlots.AddRange(reservations.Where(r =>
                (r.EffectiveStartDate == null || r.EffectiveStartDate.Value <= shiftedBusinessStartDateUtc) &&
                (r.EffectiveEndDate == null || r.EffectiveEndDate.Value >= shiftedBusinessStartDateUtc) &&
                (r.DayOfWeek.Contains(shiftedLocalDayOfWeek.ToString())) &&
                (!r.Exceptions.Contains(shiftedLocalBusinessStartDate.Date)) &&
                (
                (dayShift == 1 && r.StartTime.Hours < site.BusinessDayOffset) ||
                (dayShift == -1 && r.StartTime.Hours >= (24 + site.BusinessDayOffset))
                )
                ).Select(a => ReservedSlotMapper.MapFromReservation(a, siteTimeZone, businessStartDateUtc)).ToList());
            }

            List<Appointment> appointments = appointmentProvider.GetAppointments(db, businessStartDateUtc, businessEndDateUtc, siteId);

            List<AppointmentDockDaily> appointmentDocks = new List<AppointmentDockDaily>();

            foreach (var dock in site.Docks)
            {

                appointmentDocks.Add(_dockCapacitiesService.GetDockDailyCapacity(site, dock, requestDateUtc, appointments, reservedSlots));
            }

            return appointmentDocks;
        }

        public void UpdateAppointmentAggregates(Appointment appointment)
        {
            appointmentProvider.UpdateAppointmentAggregates(db, appointment);
        }

        public UnreservedSlotResults GetUnreservedSlots(
            int siteId, IEnumerable<SlotOrder> orders,
            int appointmentDuration, bool isCarrierUser,
            DateTime currentUTCTime, int appointmentId = 0,
            DateTime? requestedDate = null, int requestedDoorGroupId = 0,
            bool forAutoAppoint = false, List<AutoAppointDockQuota> autoAppointDockQuotas = null,
            int carrierID = 0, int deliveryCarrier = 0, int? appointmentPalletOverride = null)
        {
            var unreservedSlotResults = new UnreservedSlotResults
            {
                Messages = new List<Message>(),
                Slots = new List<UnreservedSlot>(),
                Docks = new List<AppointmentDockDaily>(),
                Vendors = new List<SlotVendor>()
            };

            Site site = db.Sites.FirstOrDefault(s => s.ID == siteId);

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(site.TimeZone);
            DateTime localCurrentTime = TimeZoneInfo.ConvertTimeFromUtc(currentUTCTime, siteTimeZone);
            DateTime localMidnight = localCurrentTime.Date;

            var vendorIDs = orders.Select(o => o.VendorID).ToList();
            vendorIDs.AddRange(orders.Where(o => o.OrderDetails != null).SelectMany(o => o.OrderDetails)
                .Select(od => od.VendorID).ToList());

            vendorIDs = vendorIDs.Distinct().ToList();

            var vendors = db.Vendors.Where(v => v.SiteID == siteId && vendorIDs.Contains(v.ID)).ToList();

            /// Determines if carrier would be able to see these UnReservedSlots too
            bool sameDayAllowedForCarrier = vendors.All(v => v.AllowSameDayAppointment);

            /// If the current user is not a carrier then they should be able to see slots on the game day 
            bool sameDayAllowed = !isCarrierUser || sameDayAllowedForCarrier;

            /// if same day is allowed then the earliest time is today, so we won't need to add days,
            /// else the earliest day is tomorrow

            var earliestSiteTime = TimeZoneInfo.ConvertTimeToUtc(localMidnight, siteTimeZone)
                .AddDays(sameDayAllowed ? 0 : 1);

            var latestSiteTime = site.AppointmentDateLimit.HasValue && !requestedDate.HasValue
                ? TimeZoneInfo.ConvertTimeToUtc(localMidnight, siteTimeZone)
                    .AddDays(site.AppointmentDateLimit.Value + 1)
                : DateTime.MaxValue;

            var doorGroupResult =
                GetDoorGroup(siteId, orders, requestedDoorGroupId, vendors, carrierID, deliveryCarrier);

            var resultsMessage = string.Empty;

            double totalUnits = OrdersService.GetOrderTotalUnitsSum(site, appointmentPalletOverride, orders);

            var availableDoors = db.Doors.Include(d => d.Dock).Where(d =>
                d.DoorGroupID == doorGroupResult.Data.DoorGroupID.Value &&
                (
                    forAutoAppoint ||
                    (d.Active && d.MinUnitCount <= totalUnits && d.MaxUnitCount >= totalUnits)
                )
            ).ToList();

            var doors = new List<SlotDoor>();

            if (forAutoAppoint)
            {
                doors.AddRange(availableDoors.Select(d => new SlotDoor
                {
                    ID = d.ID,
                    Dock = d.Dock,
                    earliestDate = currentUTCTime,
                    latestDate = currentUTCTime.AddDays(7),
                    MinUnitCount = d.MinUnitCount,
                    MaxUnitCount = d.MaxUnitCount,
                    Priority = d.Priority
                }).ToList());
            }
            else
            {
                var docks = availableDoors.Select(d => d.Dock).Distinct().ToList();

                foreach (var dock in docks)
                {
                    //Check if we got any doors that have valid ranges. If not, then the orders cannot be delivered together at any dock.

                    var validDateRange = GetValidDateRangeForDock(
                        dock,
                        orders.Select(o => o.DueDate.Value).ToList(),
                        siteTimeZone
                    );

                    if (validDateRange != null)
                    {
                        if (requestedDate.HasValue)
                        {
                            if (validDateRange.HasInside(TimeZoneInfo.ConvertTimeToUtc(requestedDate.Value,
                                    siteTimeZone)))
                            {
                                unreservedSlotResults.IsDateInSiteDeliveryWindow = true;
                                doors.AddRange(availableDoors.Where(d => d.DockID == dock.ID).Select(d => new SlotDoor
                                {
                                    ID = d.ID,
                                    Dock = dock,
                                    earliestDate = requestedDate.Value.AddDays(-1),
                                    latestDate = requestedDate.Value.AddDays(1),
                                    MinUnitCount = d.MinUnitCount,
                                    MaxUnitCount = d.MaxUnitCount,
                                    Priority = d.Priority
                                }).ToList());
                            }
                            else
                            {
                                unreservedSlotResults.Messages.Add(new Message
                                {
                                    Code = "DOCK_DATE_THRESHOLD",
                                    Text = string.Format(
                                        "Appointment date will not accommodate the orders based on their due dates and dock scheduling thresholds for dock '{0}'.",
                                        dock.Name)
                                });
                            }
                        }
                        else
                        {
                            doors.AddRange(availableDoors.Where(d => d.DockID == dock.ID).Select(d => new SlotDoor
                            {
                                ID = d.ID,
                                Dock = dock,
                                earliestDate = validDateRange.Start,
                                latestDate = validDateRange.End.AddDays(-1).AddSeconds(1),
                                MinUnitCount = d.MinUnitCount,
                                MaxUnitCount = d.MaxUnitCount,
                                Priority = d.Priority
                            }).ToList());
                        }
                    }
                    else
                    {
                        unreservedSlotResults.Messages.Add(new Message
                        {
                            Code = "DOCK_DATE_THRESHOLD",
                            Text = string.Format(
                                "No appointment date will accommodate the orders based on their due dates and dock scheduling thresholds for dock '{0}'.",
                                dock.Name)
                        });
                        continue;
                    }
                }
            }

            if (doors.Count() == 0)
            {
                return unreservedSlotResults;
            }

            var startDate = DateTime.SpecifyKind(doors.Min(d => d.earliestDate), DateTimeKind.Utc);
            var endDate = DateTime.SpecifyKind(doors.Max(d => d.latestDate), DateTimeKind.Utc);

            if (localCurrentTime.Date == requestedDate?.Date && !sameDayAllowed)
            {
                return unreservedSlotResults;
            }

            if (endDate < earliestSiteTime || startDate > latestSiteTime)
            {
                return unreservedSlotResults;
            }

            if (startDate < earliestSiteTime)
            {
                startDate = DateTime.SpecifyKind(earliestSiteTime, DateTimeKind.Utc);
            }

            //First date to try is the due date of the primary PO
            DateTime appointmentDate;

            if (requestedDate.HasValue)
            {
                appointmentDate = TimeZoneInfo.ConvertTimeToUtc(requestedDate.Value, siteTimeZone);
            }
            else
            {
                //Finding dates to try for reserved slots:
                //The date range to check 
                //          tomorrow, or the latest due date across all POs minus the early schedule threshold or 5, whichiver is smaller
                //          The earliest due date across all POs plus the late schedule threshold or 5, whichever is smaller
                //The date to start checking is the due date of the primary PO
                //If that date is outside the early/late schedule threshold of any other PO, can't use that date.
                //Then, subtract 1 day from the appointment date (unless it that appointment date is tomorrow)
                //Check that date against the early/late schedule threshold for all the other POs.
                //Then, add 2 days to the appointment date, check that one
                //Then, subtract 3 days from the appointment date, check that one
                appointmentDate =
                    DateTime.SpecifyKind(
                        orders.Where(o => o.DueDate.HasValue)
                            .OrderByDescending(o =>
                                site.UnitType.Equals(UnitTypeEnum.Pallets) ? o.PalletCount : o.CaseCount)
                            .FirstOrDefault().DueDate.Value, DateTimeKind.Utc);
            }

            if (appointmentDate < startDate)
                appointmentDate = DateTime.SpecifyKind(startDate, DateTimeKind.Utc);

            if (appointmentDate > endDate)
                appointmentDate = DateTime.SpecifyKind(endDate, DateTimeKind.Utc);

            var resStartDate = startDate.AddDays(-1);
            var resEndDate = endDate.AddDays(+1);

            var reservations = db.Reservations.AsNoFilter().Where(r =>
                    r.SiteID == siteId &&
                    r.Active &&
                    (r.EffectiveStartDate == null || r.EffectiveEndDate >= resStartDate &&
                        (r.EffectiveEndDate == null || r.EffectiveStartDate <= resEndDate))
                )
                .Include(r => r.Doors)
                .ToList();

            doors = doors.Distinct().OrderBy(d => d.Priority).ToList();
            var doorIDs = doors.Select(r => r.ID).ToList();

            var appointmentStartDate = startDate.AddDays(-1);

            var maxNumberOfDays = 10;

            DateTime appointmentEndDate =
                requestedDate.HasValue ? endDate.AddDays(1) : startDate.AddDays(maxNumberOfDays + 3);

            var appointments =
                appointmentProvider.GetAppointments(db, appointmentStartDate, appointmentEndDate, siteId);

            var allOrders = appointments.SelectMany(a => a.Orders).ToList();

            var allVendorIDs = allOrders.Select(o => o.VendorID).ToList();

            allVendorIDs = allVendorIDs.Distinct().ToList();

            var allVendors = db.Vendors.Include(v => v.EquipmentType)
                .Where(v => v.SiteID == siteId && allVendorIDs.Contains(v.ID)).Distinct().ToList();

            var equipment = db.Equipment.Where(e => e.SiteID == siteId).ToList();

            var allDoors = db.Doors.Include(d => d.Dock).Where(d => d.Dock.SiteID == siteId).Distinct().ToList();

            List<Schedule> schedules = new List<Schedule>();

            if (!forAutoAppoint)
            {
                schedules = db.Schedules.Include(s => s.Docks).Include(s => s.Doors)
                    .Where(ds =>
                        ds.EffectiveEndDate >= earliestSiteTime &&
                        ds.SiteID == siteId &&
                        ds.Active
                    ).ToList();
            }

            var increment = 1;
            int incrementMinutes = site.AppointmentInterval ?? 15;

            var dayCount = 1;

            while (dayCount <= maxNumberOfDays) //Loop through the days and build slots for each day
            {
                dayCount++;

                var localAppointmentDate = TimeZoneInfo.ConvertTimeFromUtc(appointmentDate, siteTimeZone);

                /// Check if it's not a same day appointment while sameDay is allowed and if the appointment is before the site is closed
                if ((!(localAppointmentDate.Date == localCurrentTime.Date && !sameDayAllowed)) &&
                    appointmentDate < latestSiteTime)
                {
                    double appointmentDateMillis =
                        MRUtils.getMillisFromMMDDYYYY(appointmentDate.ToString(), site.TimeZone);

                    List<DateTime> slotTimesUTC = getClientSlotsUTC(site, appointmentDate, appointmentDateMillis,
                        incrementMinutes, appointmentDuration);

                    var firstSlot = slotTimesUTC.First();
                    var lastSlot = slotTimesUTC.Last().AddMinutes(appointmentDuration);

                    var dayBlockingAppointments = appointments
                        .Where(a =>
                            (a.GateInTime == null && a.StartTime.AddMinutes(a.ScheduledDuration) >= firstSlot &&
                             a.StartTime < lastSlot) ||
                            (a.GateInTime != null && a.GateInTime.Value.AddMinutes(a.ScheduledDuration) >= firstSlot &&
                             a.GateInTime < lastSlot)
                        ).ToList();

                    var dayCapacityAppointments = dayBlockingAppointments
                        .Where(a =>
                            (a.GateInTime == null && a.StartTime >= firstSlot && a.StartTime < lastSlot) ||
                            (a.GateInTime != null && a.GateInTime >= firstSlot && a.GateInTime < lastSlot)
                        ).ToList();

                    var vendorLoads = CheckVendorLoadCounts(vendors, dayCapacityAppointments);

                    if (!forAutoAppoint && vendorLoads.Count() > 0)
                    {
                        unreservedSlotResults.Vendors.AddRange(vendorLoads);
                    }
                    else
                    {
                        var daySlots = new List<UnreservedSlot>();

                        string appointmentDOW = localAppointmentDate.DayOfWeek.GetHashCode().ToString();

                        int dayShift = 0;
                        if (site.BusinessDayOffset > 0)
                            dayShift = 1;
                        else if (site.BusinessDayOffset < 0)
                            dayShift = -1;

                        var reservedSlots = reservations.Where(r =>
                                (r.EffectiveStartDate == null || r.EffectiveStartDate.Value <= appointmentDate) &&
                                (r.EffectiveEndDate == null || r.EffectiveEndDate.Value >= appointmentDate) &&
                                r.DayOfWeek.Contains(appointmentDOW.ToString()) &&
                                !r.Exceptions.Contains(localAppointmentDate.Date) &&
                                (
                                    (
                                        site.BusinessDayOffset >= 0 && r.StartTime.Hours >= site.BusinessDayOffset
                                    ) ||
                                    (
                                        site.BusinessDayOffset < 0 && r.StartTime.Hours < 24 + site.BusinessDayOffset
                                    ))
                            )
                            .Select(x => new ReservedSlot
                            {
                                ReservationID = x.ID,
                                Doors = x.Doors,
                                DoorIDs = x.Doors.Select(d => d.ID).ToList(),
                                StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate,
                                    x.StartTime, false),
                                MinPallets = x.MinPallets,
                                MaxPallets = x.MaxPallets,
                                MinCases = x.MinCases,
                                MaxCases = x.MaxCases,
                                DurationInMinutes = x.Length
                            }).ToList();

                        var closures = schedules.Where(s =>
                                (s.EffectiveStartDate == null || s.EffectiveStartDate.Value <= appointmentDate) &&
                                (s.EffectiveEndDate == null || s.EffectiveEndDate.Value >= appointmentDate) &&
                                s.DayOfWeek.Contains(appointmentDOW.ToString()) &&
                                s.IsReceivingDay == (localCurrentTime.Date == localAppointmentDate.Date)
                            )
                            .Select(s => new
                            {
                                s.ID,
                                s.Availability,
                                s.Docks,
                                s.Doors,
                                StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate,
                                        s.StartTime, true),
                                EndTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate,
                                        s.EndTime, true),
                            }
                            )
                            .ToList();

                        if (dayShift != 0)
                        {
                            var appointmentDOWBeforeAfter =
                                localAppointmentDate.AddDays(dayShift).DayOfWeek.GetHashCode();

                            closures.AddRange(schedules.Where(r =>
                                    (r.EffectiveStartDate == null ||
                                     r.EffectiveStartDate.Value <= appointmentDate.AddDays(dayShift)) &&
                                    (r.EffectiveEndDate == null ||
                                     r.EffectiveEndDate.Value >= appointmentDate.AddDays(dayShift)) &&
                                    r.DayOfWeek.Contains(appointmentDOWBeforeAfter.ToString()) &&
                                    r.IsReceivingDay == (localCurrentTime.Date == localAppointmentDate.Date)
                                )
                                .Select(s => new
                                {
                                    s.ID,
                                    s.Availability,
                                    s.Docks,
                                    s.Doors,
                                    StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone,
                                            appointmentDate.AddDays(dayShift), s.StartTime, true),
                                    EndTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone,
                                            appointmentDate.AddDays(dayShift), s.EndTime, true),
                                }
                                )
                                .ToList());

                            reservedSlots.AddRange(reservations.Where(r =>
                                    (r.EffectiveStartDate == null ||
                                     r.EffectiveStartDate.Value <= appointmentDate.AddDays(dayShift)) &&
                                    (r.EffectiveEndDate == null ||
                                     r.EffectiveEndDate.Value >= appointmentDate.AddDays(dayShift)) &&
                                    r.DayOfWeek.Contains(appointmentDOWBeforeAfter.ToString()) &&
                                    !r.Exceptions.Contains(localAppointmentDate.AddDays(dayShift).Date) &&
                                    (
                                        (dayShift == 1 && r.StartTime.Hours < site.BusinessDayOffset) ||
                                        (dayShift == -1 && r.StartTime.Hours >= (24 + site.BusinessDayOffset))
                                    )
                                )
                                .Select(x => new ReservedSlot
                                {
                                    ReservationID = x.ID,
                                    Doors = x.Doors,
                                    DoorIDs = x.Doors.Select(d => d.ID).ToList(),
                                    StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone,
                                        appointmentDate.AddDays(dayShift), x.StartTime, false),
                                    MinPallets = x.MinPallets,
                                    MaxPallets = x.MaxPallets,
                                    MinCases = x.MinCases,
                                    MaxCases = x.MaxCases,
                                    DurationInMinutes = x.Length
                                }).ToList());
                        }

                        closures = closures.Where(c => c.EndTime != c.StartTime).Select(s => new
                        {
                            s.ID,
                            s.Availability,
                            s.Docks,
                            s.Doors,
                            s.StartTime,
                            EndTime = s.StartTime < s.EndTime ? s.EndTime : s.EndTime.AddDays(1)
                        }
                        ).ToList();

                        Dictionary<Dock, bool> dockCapacity = new Dictionary<Dock, bool>();

                        foreach (var dr in doors)
                        {
                            /// Determines if current door slots will be shown to the carrier or not
                            bool isDoorSkippedForCarrier = false;

                            if (TimeSpan.TryParse(dr.Dock.ScheduleCutoffTime, out var cutoffTimeSpan))
                            {
                                var cutoffTime =
                                    TimeZoneInfo.ConvertTimeToUtc(localMidnight.Add(cutoffTimeSpan), siteTimeZone);

                                #region comments

                                // If same day is not allowed for the carrier then they can't schedule for tomorrow if they are past cutoff time

                                /// We are checking if they are past cutoff time, if they are scheduling for tomorrow and if same day is not allowed while the user is a carrier.
                                /// If all of that was met then we don't return slots for that door 

                                /// You may wonder why are we adding 2 days instead of 1 to check if they are shceduling for tomorrow.
                                /// We are doing so, as the appointmentDate gets shifted one day when same day is not allowed, so we need to shift too by adding another day to the current date

                                #endregion

                                if (currentUTCTime > cutoffTime &&
                                    appointmentDate.Date < currentUTCTime.AddDays(2).Date)
                                {
                                    if (isCarrierUser && !sameDayAllowed)
                                    {
                                        /// Skips current door
                                        continue;
                                    }
                                    else if (!isCarrierUser && !sameDayAllowedForCarrier)
                                    {
                                        /// Can be because we are past cut off time or sameDay not Allowed
                                        isDoorSkippedForCarrier = true;
                                    }
                                }
                            }

                            if (autoAppointDockQuotas != null ||
                                (site.DockThresholdFeature.HasValue && site.DockThresholdFeature.Value))
                            {
                                if (!dockCapacity.ContainsKey(dr.Dock))
                                {
                                    AppointmentDockDaily appointmentDockDaily;
                                    appointmentDockDaily = _dockCapacitiesService.GetDockDailyCapacity(site, dr.Dock,
                                        appointmentDate, dayCapacityAppointments, reservedSlots, autoAppointDockQuotas);

                                    Appointment existingAppointment =
                                        dayBlockingAppointments.FirstOrDefault(a => a.ID == appointmentId);

                                    appointmentDockDaily.PalletChange =
                                        OrdersService.GetOrderTotalPalletSum(appointmentPalletOverride, orders);
                                    appointmentDockDaily.CaseChange = orders.Sum(o => o.CaseCount);
                                    appointmentDockDaily.ApptChange = 1;

                                    if (existingAppointment != null &&
                                        existingAppointment.Doors.Any(door => door.DockID == dr.Dock.ID))
                                    {
                                        appointmentDockDaily.PalletChange -=
                                            existingAppointment.TotalPalletCount ?? 0;
                                        appointmentDockDaily.CaseChange -= (decimal)existingAppointment.TotalCaseCount;
                                        appointmentDockDaily.ApptChange = 0;
                                    }

                                    if (requestedDate
                                        .HasValue) //Only include in response if there is a single day requested
                                    {
                                        AddDockMessages(appointmentDockDaily, isCarrierUser, isDoorSkippedForCarrier);
                                        unreservedSlotResults.Docks.Add(appointmentDockDaily);
                                    }

                                    var hasCapacity = true;

                                    if (appointmentDockDaily.UnreservedApptsLimit != null &&
                                        appointmentDockDaily.UnreservedApptsLimit + appointmentDockDaily.ReservedApptsLimit.GetValueOrDefault() -
                                        appointmentDockDaily.UnreservedApptsScheduled - appointmentDockDaily.ReservedApptsScheduled -
                                        appointmentDockDaily.ReservedApptsUnscheduled - appointmentDockDaily.ApptChange < 0
                                       )
                                    {
                                        hasCapacity = false;
                                    }

                                    if (appointmentDockDaily.UnreservedCasesLimit != null &&
                                        appointmentDockDaily.UnreservedCasesLimit + appointmentDockDaily.ReservedCasesLimit.GetValueOrDefault() -
                                        appointmentDockDaily.UnreservedCasesScheduled - appointmentDockDaily.ReservedCasesScheduled -
                                        appointmentDockDaily.ReservedCasesUnscheduled - appointmentDockDaily.CaseChange < 0
                                       )
                                    {
                                        hasCapacity = false;
                                    }

                                    if (appointmentDockDaily.UnreservedPalletsLimit != null &&
                                        appointmentDockDaily.UnreservedPalletsLimit + appointmentDockDaily.ReservedPalletsLimit.GetValueOrDefault() -
                                        appointmentDockDaily.UnreservedPalletsScheduled - appointmentDockDaily.ReservedPalletsScheduledDouble -
                                        appointmentDockDaily.ReservedPalletsUnscheduled - appointmentDockDaily.PalletChange < 0
                                       )
                                    {
                                        hasCapacity = false;
                                    }

                                    dockCapacity.Add(dr.Dock, hasCapacity);
                                }

                                if (!dockCapacity[dr.Dock])
                                    continue;
                            }

                            foreach (var st in slotTimesUTC)
                            {
                                if (reservedSlots.Any(r =>
                                        r.DoorIDs.Any(d => d == dr.ID) &&
                                        TimeExtensions.DoDateRangesOverlap(
                                            st, st.AddMinutes(appointmentDuration),
                                            r.StartTime, r.StartTime.AddMinutes(r.DurationInMinutes)
                                        )))
                                {
                                    continue;
                                }

                                if (dayBlockingAppointments.Any(a =>
                                        a.ID != appointmentId &&
                                        a.Doors.Any(d => d.ID == dr.ID) &&
                                        TimeExtensions.DoDateRangesOverlap(
                                            st, st.AddMinutes(appointmentDuration),
                                            a.GateInTime ?? a.StartTime,
                                            (a.GateInTime ?? a.StartTime).AddMinutes(a.ScheduledDuration))))
                                {
                                    continue;
                                }

                                if (closures.Any(s =>
                                        (s.Availability == Schedule.ScheduleAvailabilityEnum.Site ||
                                         s.Docks.Any(d => d.ID == dr.Dock.ID) ||
                                         s.Doors.Any(d => d.ID == dr.ID))
                                        &&
                                        TimeExtensions.DoDateRangesOverlap(
                                            st, st.AddMinutes(appointmentDuration),
                                            s.StartTime, s.EndTime)
                                    ))
                                {
                                    continue;
                                }

                                if (!forAutoAppoint && !isEquipmentAvailableForSlot(siteId, st, appointmentDuration,
                                        appointments, vendors, allVendors, equipment, allDoors))
                                {
                                    continue;
                                }

                                var slot = new UnreservedSlot();

                                int? minPallets = 0;
                                int? maxPallets = 0;
                                minPallets = dr.MinUnitCount;
                                maxPallets = dr.MaxUnitCount;
                                List<int> doorList = new List<int>();
                                doorList.Add(dr.ID);

                                slot.DoorIDs = doorList;
                                slot.StartTime = st;
                                slot.PalletLimit = minPallets.ToString() + "-" + maxPallets.ToString();

                                slot.Hash = ComputeHash(slot.StartTime, slot.DoorIDs.ToArray());

                                //AddSlotMessages(slot, isCarrierUser, localCurrentTime, sameDayAllowedForCarrier,
                                //    appointmentDate, isDoorSkippedForCarrier);

                                daySlots.Add(slot);
                            }
                        }

                        unreservedSlotResults.Slots.AddRange(daySlots.OrderBy(s => s.StartTime).ToList());
                    }
                }

                if (requestedDate.HasValue) //only a single date requested

                    break;

                #region comments

                //Move away from the original (best) appointment date
                //One day before, then one day after, then two days before, then two days after
                //But if one day before is today (since the best appointment date should be
                //after today at the earliest) then skip that day and check after
                //Keep checking until we get to a date before that won't work
                //and a date after that won't work

                #endregion

                appointmentDate = FixUTCMidnight(siteTimeZone,
                    appointmentDate.AddDays(increment % 2 == 0 ? increment : -increment));
                increment++;

                if (appointmentDate < startDate)
                {
                    appointmentDate = FixUTCMidnight(siteTimeZone,
                        appointmentDate.AddDays(increment % 2 == 0 ? increment : -increment));
                    increment++;
                }

                if (appointmentDate > endDate)
                {
                    appointmentDate = FixUTCMidnight(siteTimeZone,
                        appointmentDate.AddDays(increment % 2 == 0 ? increment : -increment));
                    increment++;
                }

                if (appointmentDate < startDate || appointmentDate > endDate)
                    break;
            } // end of while(true)

            unreservedSlotResults.Slots = unreservedSlotResults.Slots.Distinct().OrderBy(d => d.StartTime).ThenBy(d => d.PalletLimit).ToList();

            return unreservedSlotResults;
        }

        private List<Dock> GetCommonDocks(List<Dock> docks1, List<int> allDoorGroups)
        {
            var docks = new List<Dock>();
            foreach (var dock in docks1)
            {
                var commonDock = true;
                //If any door groups don't map to this dock, can't use the dock
                foreach (var dg in allDoorGroups)
                {
                    var odDocks = db.Docks.Where(d => d.Doors.Any(door => door.DoorGroupID == dg)).ToList();
                    if (!odDocks.Contains(dock))
                    {
                        commonDock = false;
                    }

                }
                if (commonDock)
                    docks.Add(dock);

            }

            return docks;
        }

        public ReservedSlotResults GetReservedSlots(int siteId, IEnumerable<SlotOrder> orders,
            int carrierId, bool isCarrierUser, int appointmentId = 0, int? appointmentDuration = null,
            DateTime? requestedDate = null,
            bool includeUnavailableSlots = false, int requestedDoorGroupId = 0, int deliveryCarrier = 0,
            int? appointmentPalletOverride = null)
        {
            bool ignoreEquipmentCheck = includeUnavailableSlots;
            bool ignoreReservationMinMaxLimit = includeUnavailableSlots;

            var result = new ReservedSlotResults
            {
                Messages = new List<Message>(),
                Slots = new List<ReservedSlot>(),
                Docks = new List<ReservationDock>(),
                Vendors = new List<SlotVendor>()
            };

            Site site = db.Sites.FirstOrDefault(s => s.ID == siteId);

            TimeZoneInfo siteTimeZone = OlsonToWindowsTimeZone.OlsonTimeZoneToTimeZoneInfo(site.TimeZone);
            var localCurrentTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, siteTimeZone);
            var localMidnight = localCurrentTime.Date;

            var vendorIDs = orders.Select(o => o.VendorID).ToList();
            vendorIDs.AddRange(orders.Where(o => o.OrderDetails != null).SelectMany(o => o.OrderDetails)
                .Select(od => od.VendorID).ToList());

            vendorIDs = vendorIDs.Distinct().ToList();

            var vendors = db.Vendors.Where(v => v.SiteID == site.ID && vendorIDs.Contains(v.ID)).ToList();

            /// Determines if carrier would be able to see these ReservedSlots too
            bool sameDayAllowedForCarrier = vendors.All(v => v.AllowSameDayAppointment);

            /// If the current user is not a carrier then they should be able to see slots on the game day 
            bool sameDayAllowed = !isCarrierUser || sameDayAllowedForCarrier;

            var earliestSiteTime = TimeZoneInfo.ConvertTimeToUtc(localMidnight, siteTimeZone)
                .AddDays(sameDayAllowed ? 0 : 1);

            var latestSiteTime = site.AppointmentDateLimit.HasValue && !requestedDate.HasValue
                ? TimeZoneInfo.ConvertTimeToUtc(localMidnight, siteTimeZone)
                    .AddDays(site.AppointmentDateLimit.Value + 1)
                : DateTime.MaxValue;

            var doorGroupResult =
                GetDoorGroup(siteId, orders, requestedDoorGroupId, vendors, carrierId, deliveryCarrier);

            var resultsMessage = string.Empty;

            if (doorGroupResult.Messages.Count > 0 || doorGroupResult.Data.DoorGroupID == null)
            {
                result.Messages.AddRange(doorGroupResult.Messages);
                return result;
            }

            var closestDoorGroup = db.DoorGroups.Include(dg => dg.Doors).Include(dg => dg.Doors.Select(d => d.Dock))
                .FirstOrDefault(dg => dg.ID.Equals(doorGroupResult.Data.DoorGroupID.Value) && dg.SiteID.Equals(siteId));

            double totalUnits = OrdersService.GetOrderTotalUnitsSum(site, appointmentPalletOverride, orders);

            var doors = new List<SlotDoor>();

            var docks = closestDoorGroup.Doors.Select(d => d.Dock).Distinct().ToList();

            var availableDoors = closestDoorGroup.Doors
                .Where(d => d.Active && d.MinUnitCount <= totalUnits && d.MaxUnitCount >= totalUnits).ToList();

            docks = availableDoors.Select(d => d.Dock).Distinct().ToList();

            //Check if we got any doors that have valid ranges. If not, then the orders cannot be delivered together at any dock.
            foreach (var dock in docks)
            {
                if (requestedDate.HasValue)
                {
                    doors.AddRange(closestDoorGroup.Doors.Where(d => d.DockID == dock.ID).Select(d => new SlotDoor
                    {
                        ID = d.ID,
                        Dock = dock,
                        earliestDate = requestedDate.Value.AddDays(-1),
                        latestDate = requestedDate.Value.AddDays(1)
                    }).ToList());
                }
                else
                {
                    var validDateRange = GetValidDateRangeForDock(
                        dock,
                        orders.Select(o => o.DueDate.Value).ToList(),
                        siteTimeZone
                    );
                    if (validDateRange != null)
                    {
                        doors.AddRange(closestDoorGroup.Doors.Where(d => d.DockID == dock.ID).Select(d => new SlotDoor
                        {
                            ID = d.ID,
                            Dock = dock,
                            earliestDate = validDateRange.Start,
                            latestDate = validDateRange.End.AddDays(-1).AddSeconds(1)
                        }).ToList());
                    }

                    else
                    {
                        result.Messages.Add(new Message
                        {
                            Code = "DOCK_DATE_THRESHOLD",
                            Text = string.Format(
                                "No appointment date will accommodate the orders based on their due dates and dock scheduling thresholds for dock '{0}'.",
                                dock.Name)
                        });
                        continue;
                    }
                }
            }

            if (localCurrentTime.Date == requestedDate?.Date && !sameDayAllowed)
            {
                return result;
            }

            if (doors.Count() == 0)
            {
                return result;
            }

            var startDate = doors.Min(d => d.earliestDate);
            var endDate = doors.Max(d => d.latestDate);

            if (endDate < earliestSiteTime || startDate > latestSiteTime)
            {
                return result;
            }

            if (startDate < earliestSiteTime)
            {
                startDate = earliestSiteTime;
            }

            DateTime appointmentDate;

            if (requestedDate.HasValue)
            {
                appointmentDate = TimeZoneInfo.ConvertTimeToUtc(requestedDate.Value, siteTimeZone);
            }
            else
            {
                #region comments

                //Finding dates to try for reserved slots:
                //The date range to check 
                //          tomorrow, or the latest due date across all POs minus the early schedule threshold or 5, whichiver is smaller
                //          The earliest due date across all POs plus the late schedule threshold or 5, whichever is smaller
                //The date to start checking is the due date of the primary PO
                //If that date is outside the early/late schedule threshold of any other PO, can't use that date.
                //Then, subtract 1 day from the appointment date (unless it that appointment date is tomorrow)
                //Check that date against the early/late schedule threshold for all the other POs.
                //Then, add 2 days to the appointment date, check that one
                //Then, subtract 3 days from the appointment date, check that one

                #endregion

                appointmentDate =
                    DateTime.SpecifyKind(
                        orders.Where(o => o.DueDate.HasValue)
                            .OrderByDescending(o =>
                                site.UnitType.Equals(UnitTypeEnum.Pallets) ? o.PalletCount : o.CaseCount)
                            .FirstOrDefault().DueDate.Value, DateTimeKind.Utc);
            }

            if (appointmentDate < startDate)
                appointmentDate = startDate;

            var effectiveRangeStart = startDate.AddDays(-1);

            var maxNumberOfDays = 10;

            DateTime effectiveRangeEnd;
            if (endDate.Year < DateTime.Now.AddYears(1).Year)
                effectiveRangeEnd = endDate.AddDays(1);
            else
                effectiveRangeEnd = startDate.AddDays(maxNumberOfDays + 1);

            var reservations = db.Reservations.AsNoFilter()
                .Include(r => r.Doors)
                .Include(r => r.Doors.Select(d => d.Dock))
                .Include(r => r.Carriers)
                .Include(r => r.Vendors)
                .Where(
                    r =>
                        r.SiteID == siteId &&
                        r.Active &&
                        (!r.Carriers.Any() || r.Carriers.Any(c => c.ID == carrierId)) &&
                        (!r.Vendors.Any() || r.Vendors.Any(v => vendorIDs.Contains(v.ID))) &&
                        r.Doors.Any(d => d.DoorGroupID == doorGroupResult.Data.DoorGroupID)
                )
                .ToList();

            if (reservations.Count() == 0)
            {
                return result;
            }

            var schedules = db.Schedules.Include(s => s.Docks).Include(s => s.Doors)
                .Where(ds =>
                    ds.EffectiveEndDate >= earliestSiteTime &&
                    ds.SiteID == siteId &&
                    ds.Active
                ).ToList();

            doors = doors.Distinct().ToList();

            var doorIDs = doors.Select(r => r.ID).ToList();

            var appointmentStartDate = startDate.AddDays(-2);
            var appointmentEndDate = startDate.AddDays(maxNumberOfDays + 3);

            var appointments =
                appointmentProvider.GetAppointments(db, appointmentStartDate, appointmentEndDate, siteId);

            var allOrders = appointments.SelectMany(a => a.Orders).ToList();

            var allVendorIDs = allOrders.Select(o => o.VendorID).ToList();

            allVendorIDs = allVendorIDs.Distinct().ToList();

            var allVendors = db.Vendors.Include(v => v.EquipmentType)
                .Where(v => v.SiteID == site.ID && allVendorIDs.Contains(v.ID)).ToList();

            var equipment = db.Equipment.Where(e => e.SiteID == siteId).ToList();

            var allDoors = db.Doors.Include(d => d.Dock).Where(d => d.Dock.SiteID == siteId).ToList();

            int incrementMinutes = site.AppointmentInterval ?? 15;
            var increment = 1;
            var dayCount = 1;

            while (dayCount <= maxNumberOfDays)
            {
                dayCount++;

                var localAppointmentDate = TimeZoneInfo.ConvertTimeFromUtc(appointmentDate, siteTimeZone);
                if ((!(localAppointmentDate.Date == localCurrentTime.Date && !sameDayAllowed)) &&
                    appointmentDate < latestSiteTime)
                {
                    var dayCapacityAppointments = appointments
                        .Where(a =>
                            (a.GateInTime == null && a.StartTime >= appointmentDate &&
                             a.StartTime < appointmentDate.AddDays(1)) ||
                            (a.GateInTime != null && a.GateInTime >= appointmentDate &&
                             a.GateInTime < appointmentDate.AddDays(1))
                        ).ToList();

                    var vendorLoads = CheckVendorLoadCounts(vendors, dayCapacityAppointments);
                    if (vendorLoads.Count() > 0)
                    {
                        result.Vendors.AddRange(vendorLoads);
                    }

                    else
                    {
                        var daySlots = new List<ReservedSlot>();

                        var appointmentDOW = localAppointmentDate.DayOfWeek.GetHashCode();

                        int dayShift = 0;
                        if (site.BusinessDayOffset > 0)
                            dayShift = 1;
                        else if (site.BusinessDayOffset < 0)
                            dayShift = -1;

                        var reservedSlots = reservations.Where(r =>
                                (r.EffectiveStartDate == null || r.EffectiveStartDate.Value <= appointmentDate) &&
                                (r.EffectiveEndDate == null || r.EffectiveEndDate.Value >= appointmentDate) &&
                                r.DayOfWeek.Contains(appointmentDOW.ToString()) &&
                                !r.Exceptions.Contains(localAppointmentDate.Date) &&
                                (
                                    (
                                        site.BusinessDayOffset >= 0 && r.StartTime.Hours >= site.BusinessDayOffset
                                    ) ||
                                    (
                                        site.BusinessDayOffset < 0 && r.StartTime.Hours < 24 + site.BusinessDayOffset
                                    ))
                            )
                            .Select(x => new ReservedSlot
                            {
                                ReservationID = x.ID,
                                Doors = x.Doors,
                                DoorIDs = x.Doors.Select(d => d.ID).ToList(),
                                StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate,
                                    x.StartTime, false),
                                MinPallets = x.MinPallets,
                                MaxPallets = x.MaxPallets,
                                MinCases = x.MinCases,
                                MaxCases = x.MaxCases,
                                DurationInMinutes = x.Length,
                                CarrierIDs = x.Carriers.Select(c => c.ID).ToArray(),
                                VendorIDs = x.Vendors.Select(v => v.ID).ToArray(),
                                PalletLimit = x.MinPallets.ToString() + "-" + x.MaxPallets.ToString(),
                                CaseLimit = x.MinCases.ToString() + "-" + x.MaxCases.ToString(),
                                Hash = ComputeHash(
                                    TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate, x.StartTime,
                                        false),
                                    x.Doors.Select(d => d.ID).ToArray())
                            }).ToList();

                        var closures = schedules.Where(s =>
                                (s.EffectiveStartDate == null || s.EffectiveStartDate.Value <= appointmentDate) &&
                                (s.EffectiveEndDate == null || s.EffectiveEndDate.Value >= appointmentDate) &&
                                s.DayOfWeek.Contains(appointmentDOW.ToString()) &&
                                s.IsReceivingDay == (localCurrentTime.Date == localAppointmentDate.Date))
                            .Select(s => new
                            {
                                s.ID,
                                s.Availability,
                                s.Docks,
                                s.Doors,
                                StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate,
                                        s.StartTime, true),
                                EndTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate,
                                        s.EndTime, true)
                            }
                            )
                            .ToList();

                        if (dayShift != 0)
                        {
                            var appointmentDOWBeforeAfter =
                                localAppointmentDate.AddDays(dayShift).DayOfWeek.GetHashCode();

                            reservedSlots.AddRange(reservations.Where(r =>
                                    (r.EffectiveStartDate == null ||
                                     r.EffectiveStartDate.Value <= appointmentDate.AddDays(dayShift)) &&
                                    (r.EffectiveEndDate == null ||
                                     r.EffectiveEndDate.Value >= appointmentDate.AddDays(dayShift)) &&
                                    r.DayOfWeek.Contains(appointmentDOWBeforeAfter.ToString()) &&
                                    !r.Exceptions.Contains(localAppointmentDate.AddDays(dayShift)) &&
                                    (
                                        (dayShift == 1 && r.StartTime.Hours < site.BusinessDayOffset) ||
                                        (dayShift == -1 && r.StartTime.Hours >= 24 + site.BusinessDayOffset)
                                    )
                                )
                                .Select(x => new ReservedSlot
                                {
                                    ReservationID = x.ID,
                                    Doors = x.Doors,
                                    DoorIDs = x.Doors.Select(d => d.ID).ToList(),
                                    StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone,
                                        appointmentDate.AddDays(dayShift),
                                        x.StartTime, false),
                                    MinPallets = x.MinPallets,
                                    MaxPallets = x.MaxPallets,
                                    MinCases = x.MinCases,
                                    MaxCases = x.MaxCases,
                                    DurationInMinutes = x.Length,
                                    CarrierIDs = x.Carriers.Select(c => c.ID).ToArray(),
                                    VendorIDs = x.Vendors.Select(v => v.ID).ToArray(),
                                    PalletLimit = x.MinPallets.ToString() + "-" + x.MaxPallets.ToString(),
                                    CaseLimit = x.MinCases.ToString() + "-" + x.MaxCases.ToString(),
                                    Hash = ComputeHash(
                                        TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone, appointmentDate, x.StartTime,
                                            false),
                                        x.Doors.Select(d => d.ID).ToArray())
                                }).ToList());

                            closures.AddRange(schedules.Where(r =>
                                    (r.EffectiveStartDate == null ||
                                     r.EffectiveStartDate.Value <= appointmentDate.AddDays(dayShift)) &&
                                    (r.EffectiveEndDate == null ||
                                     r.EffectiveEndDate.Value >= appointmentDate.AddDays(dayShift)) &&
                                    r.DayOfWeek.Contains(appointmentDOWBeforeAfter.ToString()) &&
                                    r.IsReceivingDay == (localCurrentTime.Date == localAppointmentDate.Date)
                                )
                                .Select(s => new
                                {
                                    s.ID,
                                    s.Availability,
                                    s.Docks,
                                    s.Doors,
                                    StartTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone,
                                            appointmentDate.AddDays(dayShift),
                                            s.StartTime, true),
                                    EndTime = TimeZoneHelper.GetCorrectedUTCTime(siteTimeZone,
                                            appointmentDate.AddDays(dayShift),
                                            s.EndTime, true)
                                }
                                )
                                .ToList());
                        }

                        closures = closures.Where(c => c.EndTime != c.StartTime).Select(s => new
                        {
                            s.ID,
                            s.Availability,
                            s.Docks,
                            s.Doors,
                            s.StartTime,
                            EndTime = s.StartTime < s.EndTime ? s.EndTime : s.EndTime.AddDays(1)
                        }
                        ).ToList();

                        foreach (var slot in reservedSlots)
                        {
                            if (!slot.Doors.Any(d => doors.Any(dr => dr.ID == d.ID)))
                                continue;

                            if (slot.DurationInMinutes < appointmentDuration)
                                continue;

                            if (site.UnitType.Equals(UnitTypeEnum.Cases))
                            {
                                if (slot.MinCases.HasValue && totalUnits < slot.MinCases)
                                    continue;
                                if (slot.MaxCases.HasValue && totalUnits > slot.MaxCases)
                                    continue;
                            }
                            else
                            {
                                if (slot.MinPallets.HasValue && totalUnits < slot.MinPallets)
                                    continue;
                                if (slot.MaxPallets.HasValue && totalUnits > slot.MaxPallets)
                                    continue;
                            }

                            var allSlotDoorsValid = true;

                            /// Determines if current door slots will be shown to the carrier or not
                            bool isDoorSkippedForCarrier = false;

                            foreach (var dr in slot.Doors)
                            {
                                if (!allSlotDoorsValid) continue;

                                #region comments

                                /// If the dock has cutoff time then we check if the appointment is after cutoff time, if same day is not allowed and if the person requesting the appointment 
                                /// is a carrier, if all of that is met then we skip this door and it's slots 

                                #endregion

                                if (TimeSpan.TryParse(dr.Dock.ScheduleCutoffTime, out var cutoffTimeSpan))
                                {
                                    var cutoffTime = TimeZoneInfo.ConvertTimeToUtc(localMidnight.Add(cutoffTimeSpan),
                                        siteTimeZone);

                                    #region comments

                                    // If same day is not allowed for the carrier then they can't schedule for tomorrow if they are past cutoff time

                                    /// We are checking if they are past cutoff time, if they are scheduling for tomorrow and if same day is not allowed while the user is a carrier.
                                    /// If all of that was met then we don't return slots for that door 

                                    /// You may wonder why are we adding 2 days instead of 1 to check if they are shceduling for tomorrow.
                                    /// We are doing so, as the appointmentDate gets shifted one day when same day is not allowed, so we need to shift too by adding another day to the current date

                                    #endregion

                                    if (DateTime.UtcNow > cutoffTime &&
                                        localAppointmentDate.Date < DateTime.UtcNow.AddDays(2).Date)
                                    {
                                        if (isCarrierUser && !sameDayAllowed)
                                        {
                                            allSlotDoorsValid = false;
                                            /// Skips current door
                                            continue;
                                        }
                                        else if (!isCarrierUser && !sameDayAllowedForCarrier)
                                        {
                                            /// Can be because we are past cut off time or sameDay not Allowed
                                            isDoorSkippedForCarrier = true;
                                        }
                                    }
                                }

                                if (!requestedDate.HasValue)
                                {
                                    var validDateRange = GetValidDateRangeForDock(
                                        dr.Dock,
                                        orders.Select(o => o.DueDate.Value).ToList(),
                                        siteTimeZone
                                    );
                                    if (validDateRange == null)
                                    {
                                        allSlotDoorsValid = false;
                                        continue; //out of slot
                                    }

                                    if (validDateRange.Start > appointmentDate ||
                                        validDateRange.End.AddDays(-1).AddSeconds(1) < appointmentDate)
                                    {
                                        allSlotDoorsValid = false;
                                        continue; //out of slot
                                    }
                                }

                                if (appointments.Where(a =>
                                        a.Doors.Any(d => d.ID == dr.ID)).Any(a =>
                                        a.ID != appointmentId &&
                                        TimeExtensions.DoDateRangesOverlap(
                                            slot.StartTime, slot.StartTime.AddMinutes(slot.DurationInMinutes),
                                            a.GateInTime ?? a.StartTime,
                                            (a.GateInTime ?? a.StartTime).AddMinutes(a.ScheduledDuration))))
                                {
                                    allSlotDoorsValid = false;
                                    continue; //out of slot
                                }

                                if (closures.Any(s =>
                                        (s.Availability == Schedule.ScheduleAvailabilityEnum.Site ||
                                         s.Docks.Any(d => d.ID == dr.Dock.ID) ||
                                         s.Doors.Any(d => d.ID == dr.ID))
                                        &&
                                        TimeExtensions.DoDateRangesOverlap(
                                            slot.StartTime,
                                            slot.StartTime.AddMinutes(appointmentDuration ?? slot.DurationInMinutes),
                                            s.StartTime, s.EndTime)
                                    ))
                                {
                                    allSlotDoorsValid = false;
                                    continue; //out of slot
                                }

                                if (!ignoreEquipmentCheck &&
                                    !isEquipmentAvailableForSlot(siteId, slot.StartTime,
                                        appointmentDuration ?? slot.DurationInMinutes,
                                        appointments, vendors, allVendors, equipment, allDoors))
                                {
                                    allSlotDoorsValid = false;
                                    continue; //out of slot
                                }
                            }

                            if (allSlotDoorsValid) //Slot is good! Add the slot and add the docks for all the slots
                            {
                                foreach (var dr in slot.Doors)
                                {
                                    if (!result.Docks.Any(d => d.DockID == dr.Dock.ID))
                                    {
                                        ReservationDock reservationDock =
                                            new ReservationDock(dr.Dock.ID, new List<Message>());
                                        AddDockMessages(reservationDock, isCarrierUser, isDoorSkippedForCarrier);
                                        result.Docks.Add(reservationDock);
                                    }
                                }

                                if (!daySlots.Any(ds => ds.ReservationID == slot.ReservationID))
                                {
                                    //AddSlotMessages(slot, isCarrierUser, localCurrentTime, sameDayAllowedForCarrier, appointmentDate, isDoorSkippedForCarrier);

                                    daySlots.Add(slot);
                                }
                            }
                        }

                        /// We order them based on the logical expression

                        // The slots having both CarrierIDs and VendorIDs come first at the list
                        result.Slots.AddRange(daySlots.Where(s => s.CarrierIDs.Any() && s.VendorIDs.Any())
                            .OrderBy(s => s.StartTime).ToList());

                        // Then the slots having CarrierIDs only
                        result.Slots.AddRange(daySlots.Where(s => !s.CarrierIDs.Any() && s.VendorIDs.Any())
                            .OrderBy(s => s.StartTime).ToList());

                        // At the bottom of the list we have the slots having VendorIDs only
                        result.Slots.AddRange(daySlots.Where(s => s.CarrierIDs.Any() && !s.VendorIDs.Any())
                            .OrderBy(s => s.StartTime).ToList());
                    }
                }

                if (result.Slots.Count >= site.MaximumReservationTimeSlots)
                    break;

                if (requestedDate.HasValue) //only a single date requested

                    break;

                //Move away from the original (best) appointment date
                //One day before, then one day after, then two days before, then two days after
                //But if one day before is today (since the best appointment date should be
                //after today at the earliest) then skip that day and check after
                //Keep checking until we get to a date before that won't work
                //and a date after that won't work

                appointmentDate = FixUTCMidnight(siteTimeZone,
                    appointmentDate.AddDays(increment % 2 == 0 ? increment : -increment));
                increment++;

                if (appointmentDate < startDate)
                {
                    appointmentDate = FixUTCMidnight(siteTimeZone,
                        appointmentDate.AddDays(increment % 2 == 0 ? increment : -increment));
                    increment++;
                }

                if (appointmentDate > endDate)
                {
                    appointmentDate = FixUTCMidnight(siteTimeZone,
                        appointmentDate.AddDays(increment % 2 == 0 ? increment : -increment));
                    increment++;
                }

                if (appointmentDate < startDate || appointmentDate > endDate)
                    break;
            } // end of while(true)

            result.Slots = result.Slots.OrderBy(d => d.StartTime.Date).ToList();

            if (result.Slots.Count > site.MaximumReservationTimeSlots)
                result.Slots = result.Slots.Take(site.MaximumReservationTimeSlots).ToList();

            return result;
        }

        public string GetSlotHash(int appointmentID)
        {
            var appointment = db.Appointments.Include(a => a.Doors).Where(a => a.ID == appointmentID).FirstOrDefault();

            return ComputeHash(DateTime.SpecifyKind(appointment.StartTime, DateTimeKind.Utc), appointment.Doors.Select(d => d.ID).ToArray());
        }

        public string ComputeHash(DateTime startTime, int[] doorIDs)
        {
            return startTime.Ticks + "|" + string.Join("|", doorIDs);
        }

        public bool IsSlotOccupied(Appointment appointment, Door door)
        {

            var appSearchStart = appointment.StartTime;
            var apptSearchEnd = appointment.StartTime.AddMinutes(appointment.ScheduledDuration);

            var blockingStatuses = db.AppointmentStatuses.Where(a => a.BlockSlot).Select(s => s.ID).ToList();


            //Queried with AsNoFilter to get all appointments, not just ones linked to the user's carrier.
            var result =
             db.Appointments.AsNoFilter().Where(a =>
                a.ID != appointment.ID &&
                a.Doors.Any(d => d.ID == door.ID) &&
                DbFunctions.AddMinutes(a.StartTime, a.ScheduledDuration) > appSearchStart &&
                a.StartTime < apptSearchEnd &&
                blockingStatuses.Contains((int)a.AppointmentStatusID)
              ).Any();


            return result;
        }

        public async Task<bool> IsSlotOccupiedAsync(Appointment appointment, Door door, CancellationToken cancellationToken)
        {
            var appSearchStart = appointment.StartTime;

            var apptSearchEnd = appointment.StartTime.AddMinutes(appointment.ScheduledDuration);

            var blockingStatuses = db.AppointmentStatuses.Where(a => a.BlockSlot).Select(s => s.ID).ToList();

            bool result =
                await db.Appointments.AsNoFilter().AnyAsync(a =>
                    a.ID != appointment.ID &&
                    a.Doors.Any(d => d.ID == door.ID) &&
                    DbFunctions.AddMinutes(a.StartTime, a.ScheduledDuration) > appSearchStart &&
                    a.StartTime < apptSearchEnd &&
                    blockingStatuses.Contains((int)a.AppointmentStatusID)
                , cancellationToken);
            return result;
        }

        public List<SlotVendor> CheckVendorLoadCounts(DateTime appointmentDate, int businessDayOffset, List<Vendor> vendors, List<Appointment> appointments)
        {

            var result = new List<SlotVendor>();


            var vendorOrders = appointments
                                             .Where(a =>

                                         (a.GateInTime == null && a.StartTime >= appointmentDate.AddHours(businessDayOffset) && a.StartTime < appointmentDate.AddHours(businessDayOffset).AddDays(1)) ||
                                         (a.GateInTime != null && a.GateInTime >= appointmentDate.AddHours(businessDayOffset) && a.GateInTime < appointmentDate.AddHours(businessDayOffset).AddDays(1))

                                       ).SelectMany(a => a.Orders).ToList();




            foreach (var v in vendors.Where(v => v.MaxLoadCount.HasValue))
            {



                var vendorLoadCount = appointments
                                             .Where(a =>
                                             (
                                         (a.GateInTime == null && a.StartTime >= appointmentDate.AddHours(businessDayOffset) && a.StartTime < appointmentDate.AddHours(businessDayOffset).AddDays(1)) ||
                                         (a.GateInTime != null && a.GateInTime >= appointmentDate.AddHours(businessDayOffset) && a.GateInTime < appointmentDate.AddHours(businessDayOffset).AddDays(1))
                                       )
                                                  &&
                                                 a.Orders.Any(o => o.VendorID == v.ID)
                                             ).Count();

                if (vendorLoadCount >= v.MaxLoadCount)
                {
                    result.Add(new SlotVendor
                    {
                        VendorID = v.ID,
                        Name = v.Name,
                        MaxLoadCount = v.MaxLoadCount.Value,
                        LoadCount = vendorLoadCount + 1

                    });

                }
                else
                {
                    var orderDetails = new List<OrderDetail>();


                }

            }

            return result;

        }
        public List<SlotVendor> CheckVendorLoadCounts(List<Vendor> vendors, List<Appointment> appointments)
        {

            var result = new List<SlotVendor>();

            foreach (var v in vendors.Where(v => v.MaxLoadCount.HasValue))
            {
                var vendorOver = false;

                var vendorAppointments = appointments.Where(a => a.Orders.Any(o => o.VendorID == v.ID)).Select(a => a.ID).ToList();
                if (vendorAppointments.Count >= v.MaxLoadCount)
                {
                    vendorOver = true;
                }
                else
                {
                    foreach (var a in appointments.Where(a => !vendorAppointments.Contains(a.ID)))
                    {
                        foreach (var order in a.Orders)
                        {
                            if (db.OrderDetails.Any(od => od.OrderID == order.ID && od.VendorID == v.ID))
                            {
                                vendorAppointments.Add(a.ID);
                                continue;
                            }
                        }
                        if (vendorAppointments.Count >= v.MaxLoadCount)
                        {
                            vendorOver = true;
                            continue;
                        }
                    }
                }
                if (vendorOver)
                    result.Add(new SlotVendor
                    {
                        VendorID = v.ID,
                        Name = v.Name,
                        MaxLoadCount = v.MaxLoadCount.Value,
                        LoadCount = vendorAppointments.Count + 1

                    });


            }
            return result;

        }

        private DateTime FixUTCMidnight(TimeZoneInfo timeZoneInfo, DateTime d)
        {
            return d.AddHours(-(d.Hour + timeZoneInfo.GetUtcOffset(d).TotalHours));
        }

        private void AddDockMessages(IFindSlotDock dock, bool isCarrierUser, bool isDoorSkippedForCarrier)
        {
            if (!isCarrierUser && isDoorSkippedForCarrier)
            {
                dock.Messages.Add(new Message(UnreservedSlotCode.DockCutOff,
                    "Slot is not available because the current time is past the dock cut off time"));
            }
        }

        public string ValidateAppointment(Appointment appointment, IPrincipal principal = null)
        {
            var cancelledStatus = db.AppointmentStatuses.FirstOrDefault(s => s.Code == "CA");

            if (cancelledStatus != null && appointment.AppointmentStatusID != cancelledStatus.ID)
            {
                foreach (var door in appointment.Doors)
                {
                    if (!door.Active)
                    {
                        return $"Door {door.Name} is inactive.";
                    }

                    if (principal == null || (principal != null && principal.IsInRole("carrier")))
                    {
                        if (IsSlotOccupied(appointment, door))
                        {
                            return $"Door {door.Name} is occupied.";
                        }

                    }
                }
            }

            return string.Empty;
        }

    }
}
