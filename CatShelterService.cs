using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microservices.Common.Exceptions;
using Microservices.ExternalServices.Authorization;
using Microservices.ExternalServices.Authorization.Types;
using Microservices.ExternalServices.Billing;
using Microservices.ExternalServices.Billing.Types;
using Microservices.ExternalServices.CatDb;
using Microservices.ExternalServices.CatDb.Types;
using Microservices.ExternalServices.CatExchange;
using Microservices.ExternalServices.CatExchange.Types;
using Microservices.ExternalServices.Database;
using Microservices.Types;

namespace Microservices
{
    public class CatShelterService : ICatShelterService
    {
        private readonly IDatabase _db;
        private readonly IAuthorizationService _authorizationService;
        private readonly IBillingService _billingService;
        private readonly ICatInfoService _catInfoService;
        private readonly ICatExchangeService _catExchangeService;

        public CatShelterService(
            IDatabase database,
            IAuthorizationService authorizationService,
            IBillingService billingService,
            ICatInfoService catInfoService,
            ICatExchangeService catExchangeService)
        {
            _db = database;
            _authorizationService = authorizationService;
            _billingService = billingService;
            _catInfoService = catInfoService;
            _catExchangeService = catExchangeService;
        }

        public async Task<List<Cat>> GetCatsAsync(string sessionId, int skip, int limit, CancellationToken cancellationToken)
        {
            var user = await TryTwice<AuthorizationResult>(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));
            if (!user.IsSuccess)
                throw new AuthorizationException();

            var catsInShelter = _db.GetCollection<CatEntity, Guid>("Cats");

            var billing = await TryTwice<List<Product>>(() => _billingService.GetProductsAsync(skip, limit, cancellationToken));

            var cats = new List<Cat>();
            foreach (var product in billing)
            {
                var catEntity = await TryTwice<CatEntity>(() => catsInShelter.FindAsync(product.Id, cancellationToken));
                cats.Add(catEntity as Cat);
            }

            return cats;
        }

        public async Task AddCatToFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryTwice<AuthorizationResult>(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));
            if (!user.IsSuccess)
                throw new AuthorizationException();

            var favorites = _db.GetCollection<UserFavorites, Guid>("Favorites");

            var userFavorites = await TryTwice<UserFavorites>(() => favorites.FindAsync(user.UserId, cancellationToken));

            if (userFavorites == null)
            {
                userFavorites = new UserFavorites { Id = user.UserId, Favorites = new HashSet<Guid>() };
                await TryTwice(() => favorites.WriteAsync(userFavorites, cancellationToken));
            }

            userFavorites.Favorites.Add(catId);
        }

        public async Task<List<Cat>> GetFavouriteCatsAsync(string sessionId, CancellationToken cancellationToken)
        {
            var user = await TryTwice<AuthorizationResult>(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));
            if (!user.IsSuccess)
                throw new AuthorizationException();

            var favorites = _db.GetCollection<UserFavorites, Guid>("Favorites");

            var userFavorites = await TryTwice<UserFavorites>(() => favorites.FindAsync(user.UserId, cancellationToken));

            var catsInShelter = _db.GetCollection<CatEntity, Guid>("Cats");

            var cats = new List<Cat>();

            if (userFavorites != null && userFavorites.Favorites != null)
            {
                foreach (var id in userFavorites.Favorites)
                {
                    var cat = await TryTwice<Product>(() => _billingService.GetProductAsync(id, cancellationToken));
                    if (cat != null)
                    {
                        var catEntity = await TryTwice<CatEntity>(() => catsInShelter.FindAsync(id, cancellationToken));
                        cats.Add(catEntity as Cat);
                    }
                }
            }

            return cats;
        }

        public async Task DeleteCatFromFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryTwice<AuthorizationResult>(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));
            if (!user.IsSuccess)
                throw new AuthorizationException();

            var favorites = _db.GetCollection<UserFavorites, Guid>("Favorites");

            var userFavorites = await TryTwice<UserFavorites>(() => favorites.FindAsync(user.UserId, cancellationToken));

            if (userFavorites != null && userFavorites.Favorites != null)
            {
                userFavorites.Favorites.Remove(catId);
            }
        }

        public async Task<Bill> BuyCatAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryTwice<AuthorizationResult>(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));
            if (!user.IsSuccess)
                throw new AuthorizationException();

            var product = await TryTwice<Product>(() => _billingService.GetProductAsync(catId, cancellationToken));

            if (product == null)
                throw new InvalidRequestException();

            var catsInShelter = _db.GetCollection<CatEntity, Guid>("Cats");

            var cat = await TryTwice<CatEntity>(() => catsInShelter.FindAsync(catId, cancellationToken));

            if (cat == null)
                return null;

            var bill = await TryTwice<Bill>(() => _billingService.SellProductAsync(catId, cat.Price, cancellationToken));

            return bill;
        }

        public async Task<Guid> AddCatAsync(string sessionId, AddCatRequest request, CancellationToken cancellationToken)
        {
            var user = await TryTwice<AuthorizationResult>(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));
            
            if (!user.IsSuccess)
                throw new AuthorizationException();

            var catInfo = await TryTwice<CatInfo>(() => _catInfoService.FindByBreedNameAsync(request.Breed, cancellationToken));

            var priceInfo = await TryTwice<CatPriceHistory>(() => _catExchangeService.GetPriceInfoAsync(catInfo.BreedId, cancellationToken));

            var latestPrice = priceInfo.Prices != null && priceInfo.Prices.Count > 0
                ? priceInfo.Prices.Last()
                : new ExternalServices.CatExchange.Types.CatPriceInfo { Date = DateTime.Now, Price = 1000 };

            var cat = new Cat()
            {
                Id = Guid.NewGuid(),
                BreedId = catInfo.BreedId,
                AddedBy = user.UserId,
                Breed = request.Breed,
                Name = request.Name,
                CatPhoto = request.Photo,
                BreedPhoto = catInfo.Photo,
                Price = latestPrice.Price,
                Prices = priceInfo.Prices.Select(p => (p.Date, p.Price)).ToList()
            };

            await TryTwice(() => _billingService.AddProductAsync(new Product { Id = cat.Id, BreedId = cat.BreedId }, cancellationToken));

            var cats = _db.GetCollection<CatEntity, Guid>("Cats");
            
            await TryTwice(() => cats.WriteAsync(new CatEntity(cat), cancellationToken));

            return cat.Id;
        }

        private static async Task<T> TryTwice<T>(Func<Task<T>> func)
        {
            try
            {
                return await func();
            }
            catch (ConnectionException)
            {
                try
                {
                    return await func();
                }
                catch(ConnectionException)
                {
                    throw new InternalErrorException();
                }
            }
        }

        private static async Task TryTwice(Func<Task> func)
        {
            try
            {
                await func();
            }
            catch (ConnectionException)
            {
                try
                {
                    await func();
                }
                catch (ConnectionException)
                {
                    throw new InternalErrorException();
                }
            }
        }

        private class CatEntity : Cat, IEntityWithId<Guid>
        {
            public CatEntity(Cat cat)
            {
                Id = cat.Id;
                BreedId = cat.BreedId;
                AddedBy = cat.AddedBy;
                Breed = cat.Breed;
                Name = cat.Name;
                CatPhoto = cat.CatPhoto;
                BreedPhoto = cat.BreedPhoto;
                Price = cat.Price;
                Prices = cat.Prices;
            }
        }

        private class UserFavorites : IEntityWithId<Guid>
        {
            public Guid Id { get; set; }
            public HashSet<Guid> Favorites { get; set; }
        }
    }
}